package client

import (
	"context"
	"net/url"
	"testing"
	"time"

	"github.com/golang/protobuf/ptypes/empty"
	control "github.com/longbridge/openapi-protobufs/gen/go/control"
	"github.com/stretchr/testify/assert"

	protocol "github.com/longbridge/openapi-protocol/go"
)

func init() {
	RegisterDialer("mock", DialConnFunc(mockDialer))
}

func mockDialer(ctx context.Context, _ protocol.Logger, uri *url.URL, handshake *protocol.Handshake, opts *DialOptions) (ClientConn, error) {
	qctx := protocol.NewContext(ctx, protocol.ClientSide)

	qctx.Platform = handshake.Platform
	qctx.Version = handshake.Version
	qctx.Codec = handshake.Codec

	return &mockConn{
		ctx:      qctx,
		packetCh: make(chan *protocol.Packet, 1),
	}, nil
}

var _ ClientConn = &mockConn{}

type mockConn struct {
	ctx *protocol.Context

	packetCh chan *protocol.Packet

	onAuth      func(*protocol.Packet) *protocol.Packet
	onReconnect func(*protocol.Packet) *protocol.Packet
	authInfo    *control.AuthResponse
	onPacket    func(*protocol.Packet) *protocol.Packet
	onPush      func(*protocol.Packet)
	writeError  error

	closed bool
}

func (c *mockConn) NeedHandleControl() bool {
	return true
}

func (c *mockConn) Close(err error) {
	c.closed = true
	close(c.packetCh)
}

func (c *mockConn) OnClose(fn func(error)) {}

func (c *mockConn) OnPacket(fn func(*protocol.Packet, error)) {
	go func() {
		for p := range c.packetCh {
			fn(p, nil)
		}
	}()

}

func (c *mockConn) Write(p *protocol.Packet, opts ...protocol.PackOption) error {
	if c.closed {
		return errConnClosed
	}

	if p.IsPing() {
		rp := protocol.MustNewResponse(c.ctx, uint32(control.Command_CMD_HEARTBEAT), protocol.StatusSuccess, p.Body, protocol.WithRequestId(p.Metadata.RequestId))

		go func() {
			time.Sleep(time.Second)
			c.packetCh <- &rp
		}()

		return nil
	}

	if p.IsAuth() {
		rp := &protocol.Packet{}
		if c.onAuth != nil {
			rp = c.onAuth(p)
		} else {
			info := c.authInfo

			if info == nil {
				info = &control.AuthResponse{
					SessionId: "session",
					Expires:   time.Now().Add(time.Minute*5).UnixNano() / int64(time.Millisecond),
				}
			}

			*rp = protocol.MustNewResponse(c.ctx, uint32(control.Command_CMD_AUTH), protocol.StatusSuccess, info, protocol.WithRequestId(p.Metadata.RequestId))
		}

		go func() {
			time.Sleep(time.Second)
			c.packetCh <- rp

		}()
		return nil
	}

	if p.IsReconnect() {
		rp := &protocol.Packet{}

		if c.onReconnect != nil {
			rp = c.onReconnect(p)
		} else {
			info := c.authInfo

			if info == nil {
				info = &control.AuthResponse{
					SessionId: "session",
					Expires:   time.Now().Add(time.Minute*5).UnixNano() / int64(time.Millisecond),
				}
			}

			*rp = protocol.MustNewResponse(c.ctx, uint32(control.Command_CMD_RECONNECT), protocol.StatusSuccess, info, protocol.WithRequestId(p.Metadata.RequestId))
		}

		go func() {
			time.Sleep(time.Second)
			c.packetCh <- rp
		}()

		return nil

	}

	if p.IsPong() {
		return nil
	}

	if p.Metadata.Type == protocol.PushPacket {
		if c.onPush != nil {
			c.onPush(p)
		}

		return nil
	}

	if c.onPacket != nil {
		go func() {
			time.Sleep(time.Second)
			c.packetCh <- c.onPacket(p)
		}()

		return nil
	}

	rp := &protocol.Packet{}
	*rp = *p
	rp.Metadata.Type = protocol.ResponsePacket

	go func() {
		time.Sleep(time.Second)
		c.packetCh <- rp
	}()

	return nil
}

func (c *mockConn) Context() *protocol.Context {
	return c.ctx
}

func newClientAndDial(opts ...DialOption) (Client, error) {
	c := New()

	err := c.Dial(context.Background(), "mock://127.0.0.1", &protocol.Handshake{
		Platform: protocol.PlatformServer,
		Codec:    protocol.CodecProtobuf,
		Version:  1,
	}, opts...)

	return c, err
}
func TestClientDialer(t *testing.T) {
	_, err := newClientAndDial()

	assert.Nil(t, err)
}

func TestClientAuth(t *testing.T) {
	c, _ := newClientAndDial()
	cli := c.(*client)
	mc := cli.conn.(*mockConn)

	cli.dialOptions.AuthTokenGetter = func() (string, error) {
		return "test auth", nil
	}
	info := &control.AuthResponse{
		SessionId: "session",
		Expires:   time.Now().Add(time.Minute*2).UnixNano() / int64(time.Millisecond),
	}

	mc.authInfo = info

	err := cli.auth()
	assert.Nil(t, err)

	assert.Equal(t, info.SessionId, cli.authInfo.SessionId)
	assert.Equal(t, info.Expires, cli.authInfo.Expires)
}

func TestClientReconnect(t *testing.T) {
	c, _ := newClientAndDial(MaxReconnect(1))
	cli := c.(*client)

	cli.authInfo = &control.AuthResponse{
		SessionId: "sess",
		Expires:   time.Now().Add(time.Minute*2).UnixNano() / int64(time.Millisecond),
	}

	err := cli.reconnect()
	assert.Nil(t, err)
	assert.Equal(t, "session", cli.authInfo.SessionId)
}

func TestClientSubscribe(t *testing.T) {
	c, _ := newClientAndDial()
	cli := c.(*client)
	mc := cli.conn.(*mockConn)

	testCmd := uint32(111)

	body := &empty.Empty{}

	waitCh := make(chan struct{}, 1)

	cli.Subscribe(testCmd, func(p *protocol.Packet) {
		var e empty.Empty
		err := p.Unmarshal(&e)
		assert.Nil(t, err)

		assert.Equal(t, testCmd, p.CMD())

		waitCh <- struct{}{}
	})

	p := protocol.MustNewPush(mc.ctx, testCmd, body)
	mc.packetCh <- &p
	<-waitCh
}

func TestClientOnPingPong(t *testing.T) {
	c, _ := newClientAndDial(Keepalive(time.Second * 3))
	cli := c.(*client)
	mc := cli.conn.(*mockConn)

	waitCh := make(chan struct{}, 1)

	cli.OnPing(func(p *protocol.Packet) {
		waitCh <- struct{}{}
	})

	cli.OnPong(func(p *protocol.Packet) {
		var data control.Heartbeat

		err := p.Unmarshal(&data)
		assert.Nil(t, err)

		waitCh <- struct{}{}
	})

	ping := protocol.MustNewRequest(mc.ctx, uint32(control.Command_CMD_HEARTBEAT), nil)
	mc.packetCh <- &ping

	<-waitCh
	<-waitCh
}
