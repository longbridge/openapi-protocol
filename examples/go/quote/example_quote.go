package main

import (
	"context"
	"flag"
	"log"

	quote "github.com/longbridge/openapi-protobufs/gen/go/quote"
	protocol "github.com/longbridge/openapi-protocol/go"
	"github.com/longbridge/openapi-protocol/go/client"
	_ "github.com/longbridge/openapi-protocol/go/v1"
)

func main() {
	var (
		addr  string
		token string
		lvl   string
	)

	flag.StringVar(&addr, "addr", "tcp://openapi-quote.longbridgeapp.com:2020", "set server address")
	flag.StringVar(&token, "token", "", "set one time password")
	flag.StringVar(&lvl, "lvl", "info", "set log level")

	flag.Parse()

	if token == "" {
		log.Fatal("token is empty")
	}

	cli := client.New()

	cli.Logger.SetLevel(lvl)
	tokenGetter := func() (string, error) {
		return token, nil
	}
	// dial and auth
	if err := cli.Dial(context.Background(), addr, &protocol.Handshake{
		Version:  1,
		Codec:    protocol.CodecProtobuf,
		Platform: protocol.PlatformOpenapi,
	}, client.WithAuthTokenGetter(tokenGetter)); err != nil {
		log.Fatalf("failed dial to server: %s, err: %v", addr, err)
	}

	cli.Logger.Info("success connect to server")

	// 1. do query request
	res, err := cli.Do(context.Background(), &client.Request{
		Cmd: uint32(quote.Command_QuerySecurityStaticInfo),
		Body: &quote.MultiSecurityRequest{
			Symbol: []string{"700.HK", "AAPL.US"},
		},
	})

	// handle err for client exception
	if err != nil {
		log.Fatal(err)
	}

	// handle server response error
	if err := res.Err(); err != nil {
		log.Fatal(err)
	}

	var infos quote.SecurityStaticInfoResponse

	// unmarshal data
	if rerr := res.Unmarshal(&infos); rerr != nil {
		log.Fatal(rerr)
	}

	cli.Logger.Infof("get infos: %v", infos)

	// 2. do subscribe
	// 2.1 subscribe price
	cli.Subscribe(uint32(quote.Command_PushQuoteData), func(p *protocol.Packet) {
		var q quote.PushQuote

		if err := p.Unmarshal(&q); err != nil {
			cli.Logger.Errorf("invalid push price data, err: %v", err)
			return
		}

		cli.Logger.Infof("receive price: %v", q)
	})

	// 2.2 subscribe depth
	cli.Subscribe(uint32(quote.Command_PushDepthData), func(p *protocol.Packet) {
		var q quote.PushDepth

		if err := p.Unmarshal(&q); err != nil {
			cli.Logger.Errorf("invalid push depth data, err: %v", err)
			return
		}

		cli.Logger.Infof("receive depth: %v", q)

	})

	// 2.3 subscribe brokers
	cli.Subscribe(uint32(quote.Command_PushBrokersData), func(p *protocol.Packet) {
		var q quote.PushBrokers

		if err := p.Unmarshal(&q); err != nil {
			cli.Logger.Errorf("invalid push brokers, err: %v", err)
			return
		}

		cli.Logger.Infof("receive brokers: %v", q)
	})

	// 2.3 subscribe trade
	cli.Subscribe(uint32(quote.Command_PushTradeData), func(p *protocol.Packet) {
		var q quote.PushTrade

		if err := p.Unmarshal(&q); err != nil {
			cli.Logger.Errorf("invalid push trade, err: %v", err)
			return
		}

		cli.Logger.Infof("receive trade: %v", q)
	})

	// 3. do subscribe quote request
	res, err = cli.Do(context.Background(), &client.Request{
		Cmd: uint32(quote.Command_Subscribe),
		Body: &quote.SubscribeRequest{
			Symbol:  []string{"700.HK"},
			SubType: []quote.SubType{quote.SubType_QUOTE, quote.SubType_DEPTH, quote.SubType_BROKERS, quote.SubType_TRADE},
		},
	})

	// handle err for client exception
	if err != nil {
		log.Fatal(err)
	}

	// handle server response error
	if rerr := res.Err(); rerr != nil {
		log.Fatal(rerr)
	}

	var subs quote.SubscriptionResponse

	if err = res.Unmarshal(&subs); err != nil {
		log.Fatal(err)
	}

	cli.Logger.Infof("success subscribe: %v", subs)

	waitCh := make(chan error)

	cli.OnClose(func(err error) {
		waitCh <- err
	})

	if err, ok := <-waitCh; ok {
		cli.Logger.Errorf("close for %v", err)
	}

	cli.Logger.Info("client closed")
}
