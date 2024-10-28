package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/big"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

const (
	urlSymbols        = "https://api.bybit.com/v5/market/instruments-info/?category=spot"
	wsURL             = "wss://stream.bybit.com/v5/public/spot"
	pingInterval      = 20 * time.Second
	minReconnectDelay = 2 * time.Second
	maxReconnectDelay = 10 * time.Second
	maxRetries        = 60
	batchSize         = 10
)

type Rate struct {
	Symbol string
	From   string
	To     string
	Buy    *big.Float
	Sell   *big.Float
}

type Symbol struct {
	Symbol    string `json:"symbol"`
	Status    string `json:"status"`
	BaseCoin  string `json:"baseCoin"`
	QuoteCoin string `json:"quoteCoin"`
}

type Parser struct {
	conn           *websocket.Conn
	symbols        []Symbol
	reconnectCount int
	mutext         sync.RWMutex
	status         string
	done           chan bool
	emitter        func(rate Rate)
	pingTimer      *time.Timer
}

type (
	WSMessage struct {
		Op   string   `json:"op"`
		Args []string `json:"args"`
	}

	SymbolResponse struct {
		Result struct {
			List []Symbol `json:"list"`
		} `json:"result"`
	}

	TickerResponse struct {
		Topic string `json:"topic"`
		Data  struct {
			Symbol string `json:"symbol"`
			Price  string `json:"lastPrice"`
		} `json:"data"`
	}
)

func initParser(emmiter func(rate Rate)) *Parser {
	return &Parser{
		status:  "initializing",
		emitter: emmiter,
		done:    make(chan bool),
	}
}

func (p *Parser) fetchSymbols() error {
	res, err := http.Get(urlSymbols)

	if err != nil {
		return err
	}

	defer res.Body.Close()

	var resSymbols SymbolResponse

	if err := json.NewDecoder(res.Body).Decode(&resSymbols); err != nil {
		return err
	}

	for _, sym := range resSymbols.Result.List {
		if sym.Status == "Trading" {
			p.symbols = append(p.symbols, sym)
		}
	}

	return nil
}

func (p *Parser) WSConnect(ctx context.Context) error {
	dialer := websocket.Dialer{
		HandshakeTimeout: 10 * time.Second,
	}

	conn, _, err := dialer.Dial(wsURL, nil)
	if err != nil {
		return err
	}

	p.conn = conn
	p.status = "connected"

	go p.readLoop(ctx)
	go p.writeLoop(ctx)

	if err := p.subscribe(); err != nil {
		return err
	}

	return nil
}

func (p *Parser) subscribe() error {
	for i := 0; i < len(p.symbols); i += batchSize {
		end := i + batchSize
		if end > len(p.symbols) {
			end = len(p.symbols)
		}

		batch := p.symbols[i:end]
		args := make([]string, len(batch))
		for j, symbol := range batch {
			args[j] = "tickers." + symbol.Symbol
		}

		msg := WSMessage{
			Op:   "subscribe",
			Args: args,
		}

		data, err := json.Marshal(msg)
		if err != nil {
			return err
		}

		if err := p.conn.WriteMessage(websocket.TextMessage, data); err != nil {
			return err
		}

		// Add a small delay between batches to avoid rate limiting
		time.Sleep(500 * time.Millisecond)
	}

	return nil
}

func (p *Parser) readLoop(ctx context.Context) error {
	defer func() {
		p.conn.Close()
		p.status = "disconnected"
		p.done <- true

		if err := p.reconnect(ctx); err != nil {
			fmt.Printf("Error reconnecting: %s\n", err)
		}

	}()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.done:
			return nil
		default:

			_, data, err := p.conn.ReadMessage()
			if err != nil {
				return err
			}

			if err := p.handleMessage(data); err != nil {
				fmt.Printf("Error handling message: %s\n", err)
				continue
			}
		}
	}
}

func (p *Parser) handleMessage(data []byte) error {
	var msg TickerResponse

	if err := json.Unmarshal(data, &msg); err != nil {
		return err
	}

	// get the symbols

	for _, sym := range p.symbols {
		if sym.Symbol == msg.Data.Symbol {
			price, ok := new(big.Float).SetString(msg.Data.Price)

			if !ok {
				return fmt.Errorf("error converting price to big.Float")
			}

			ONE := big.NewFloat(1)
			inversePrice := new(big.Float).Quo(ONE, price)

			rate := Rate{
				Symbol: sym.Symbol,
				From:   sym.BaseCoin,
				To:     sym.QuoteCoin,
				Buy:    price,
				Sell:   inversePrice,
			}

			p.emitter(rate)
		}
	}

	return nil
}

func (p *Parser) writeLoop(ctx context.Context) error {
	p.pingTimer = time.NewTimer(pingInterval)
	defer p.pingTimer.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.done:
			return nil
		case <-p.pingTimer.C:
			if err := p.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return err
			}
		}
		p.pingTimer.Reset(pingInterval)
	}
}

func (p *Parser) reconnect(ctx context.Context) error {
	p.mutext.Lock()
	p.reconnectCount++
	count := p.reconnectCount
	p.mutext.Unlock()

	delay := time.Duration(count) * minReconnectDelay

	if delay > maxReconnectDelay {
		delay = maxReconnectDelay
	}

	select {
	case <-time.After(delay):
		if err := p.WSConnect(ctx); err != nil {
			fmt.Printf("Error reconnecting: %s\n",
				err)
			p.reconnect(ctx)
		}
	case <-ctx.Done():
		return nil
	case <-p.done:
		return nil
	}

	return nil

}

func (p *Parser) Start(ctx context.Context) error {
	if err := p.fetchSymbols(); err != nil {
		return err
	}

	if err := p.WSConnect(ctx); err != nil {
		return err
	}

	return nil
}

func (p *Parser) Stop() {
	close(p.done)
	if p.conn != nil {
		p.conn.Close()
	}
	if p.pingTimer != nil {
		p.pingTimer.Stop()
	}

}

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	parser := initParser(func(rate Rate) {
		log.Printf("%s/%s: Buy: %v, Sell: %v",
			rate.From, rate.To,
			rate.Buy.String(), rate.Sell.String())
	})

	if err := parser.Start(ctx); err != nil {
		panic(err)
	}

	defer parser.Stop()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
}
