package main

import (
	"context"
	"flag"
	"log"

	trade "github.com/longbridge/openapi-protobufs/gen/go/trade"

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

	flag.StringVar(&addr, "addr", "wss://openapi-trade.longbridgeapp.com", "set server address")
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

	// do request
	res, err := cli.Do(context.Background(), &client.Request{
		Cmd: uint32(trade.Command_CMD_SUB),
		Body: &trade.Sub{
			Topics: []string{"private", "notice"},
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

	var subs trade.SubResponse

	// unmarshal data
	if err := res.Unmarshal(&subs); err != nil {
		log.Fatal(err)
	}

	cli.Logger.Infof("current subscribes: %+v", subs)

	// subscribe notify
	cli.Subscribe(uint32(trade.Command_CMD_NOTIFY), func(p *protocol.Packet) {
		var n trade.Notification

		if err := p.Unmarshal(&n); err != nil {
			cli.Logger.Errorf("invalid notification content, unmarshal error: %v", err)
			return
		}

		cli.Logger.Infof("receive notification, topic: %s, content-type: %v, data: %s", n.Topic, n.ContentType, n.Data)
	})

	waitCh := make(chan error)

	cli.OnClose(func(err error) {
		waitCh <- err
	})

	if err, ok := <-waitCh; ok {
		cli.Logger.Errorf("close for %v", err)
	}

	cli.Logger.Info("client closed")
}
