package client

import (
	"context"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	control "github.com/longbridge/openapi-protobufs/gen/go/control"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"

	protocol "github.com/longbridge/openapi-protocol/go"
	_ "github.com/longbridge/openapi-protocol/go/v1"
)

func init() {
	RegisterDialer("wss", DialConnFunc(dialWSConn))
	RegisterDialer("ws", DialConnFunc(dialWSConn))
}

var ErrInvalidMessage = errors.New("invlaid websocket message")

func dialWSConn(ctx context.Context, logger protocol.Logger, uri *url.URL, handshake *protocol.Handshake, o *DialOptions) (ClientConn, error) {
	ver := handshake.Version
	p, err := protocol.GetProtocol(ver)

	if err != nil {
		return nil, err
	}

	query := url.Values{}
	query.Set("version", strconv.FormatUint(uint64(ver), 10))
	query.Set("codec", "1")
	query.Set("platform", "9")

	uri.RawQuery = query.Encode()

	dialer := websocket.Dialer{
		Proxy:            http.ProxyFromEnvironment,
		HandshakeTimeout: o.Timeout,
	}

	conn, _, err := dialer.DialContext(ctx, uri.String(), nil)

	if err != nil {
		return nil, err
	}

	qctx := protocol.NewContext(ctx, protocol.ClientSide)
	qctx.Codec = handshake.Codec
	qctx.Platform = handshake.Platform
	qctx.Version = handshake.Version

	c := &wsConn{
		logger:        logger,
		qctx:          qctx,
		p:             p,
		conn:          conn,
		writeCh:       make(chan []byte, o.WriteQueueSize),
		dopts:         *o,
		closeCh:       make(chan struct{}),
		closeCallback: newCloseCallback(),
	}

	c.conn.SetCloseHandler(c.onClose)
	c.conn.SetPingHandler(c.onPing)
	c.conn.SetPongHandler(c.onPong)

	c.closeCh = make(chan struct{})

	c.communicating()

	return c, nil
}

var _ ClientConn = &wsConn{}

// tcp conn
type wsConn struct {
	onPacketOnce sync.Once
	closeOnce    sync.Once
	*closeCallback
	conn   *websocket.Conn
	logger protocol.Logger
	qctx   *protocol.Context
	p      protocol.Protocol

	closeCh chan struct{}

	writeCh chan []byte

	packetCh chan *protocol.Packet

	dopts DialOptions
}

func (conn *wsConn) NeedHandleControl() bool {
	return false
}

func (conn *wsConn) Context() *protocol.Context {
	return conn.qctx
}

func (conn *wsConn) Write(p *protocol.Packet, popts ...protocol.PackOption) error {
	if conn.closed() {
		return errConnClosed
	}

	if p.IsPing() {
		return conn.writePing(p)
	}

	if p.IsClose() {
		return conn.writeClose(p)
	}

	data, err := conn.p.Pack(conn.qctx, p, popts...)

	if err != nil {
		return err
	}

	return conn.write(data)
}

func (conn *wsConn) writePing(p *protocol.Packet) error {
	if conn.closed() {
		return errConnClosed
	}
	conn.logger.Debug("send ping")
	return conn.conn.WriteControl(websocket.PingMessage, p.Body, time.Now().Add(time.Second*3))
}
func (conn *wsConn) writeClose(p *protocol.Packet) error {
	if conn.closed() {
		return errConnClosed
	}
	return conn.conn.WriteControl(websocket.CloseMessage, p.Body, time.Now().Add(time.Second*3))
}

func (conn *wsConn) write(data []byte) error {
	if conn.closed() {
		return errConnClosed
	}

	select {
	case conn.writeCh <- data:
		return nil
	default:
	}

	return errors.Errorf("write queue full, len: %d", len(conn.writeCh))
}

func (conn *wsConn) OnPacket(fn func(*protocol.Packet, error)) {
	// OnPacket can only invoke once
	conn.onPacketOnce.Do(func() {
		conn.packetCh = make(chan *protocol.Packet, conn.dopts.ReadQueueSize)

		go func() {
			defer close(conn.packetCh)

			for {
				if conn.closed() {
					// consume all packet
					if l := len(conn.packetCh); l > 0 {
						for i := 0; i < l; i++ {
							p := <-conn.packetCh
							fn(p, nil)
						}

					}

					fn(nil, errConnClosed)
					return
				}

				p := <-conn.packetCh

				fn(p, nil)
			}
		}()
	})
}

func (conn *wsConn) Close(err error) {
	if conn.closed() {
		return
	}
	// Close can only invoke once
	conn.closeOnce.Do(func() {
		conn.logger.Errorf("close conn, err: %v", err)
		close(conn.closeCh)
		close(conn.writeCh)

		_ = conn.conn.Close()

		conn.DispatchClose(err)
	})
}

func (conn *wsConn) communicating() {
	go conn.reading()
	go conn.writing()
}

func (conn *wsConn) closed() bool {
	select {
	case <-conn.closeCh:
		return true
	default:
	}
	return false
}

func (conn *wsConn) onClose(code int, message string) error {
	p := protocol.MustNewPush(conn.qctx, uint32(control.Command_CMD_CLOSE), &control.Close{
		Code:   control.Close_Code(code),
		Reason: message,
	})
	conn.addPacket(&p)
	return nil
}

func (conn *wsConn) onPing(data string) error {
	conn.logger.Debug("receive ping")
	if err := conn.conn.WriteControl(websocket.PongMessage, []byte(data), time.Now().Add(time.Second*3)); err != nil {
		return err
	}
	conn.logger.Debug("success pong back")

	p := protocol.MustNewRequest(conn.qctx, uint32(control.Command_CMD_HEARTBEAT), []byte(data))
	conn.addPacket(&p)
	return nil
}

func (conn *wsConn) onPong(data string) error {
	conn.logger.Debug("receive pong")

	p := protocol.MustNewResponse(conn.qctx, uint32(control.Command_CMD_HEARTBEAT), 0, []byte(data))

	var beat control.Heartbeat
	if err := proto.Unmarshal([]byte(data), &beat); err == nil {
		if beat.HeartbeatId != nil {
			p.Metadata.RequestId = uint32(beat.GetHeartbeatId())
		}
	}

	conn.addPacket(&p)
	return nil
}

func (conn *wsConn) reading() {
	for {
		if conn.closed() {
			return
		}

		t, r, err := conn.conn.NextReader()

		if err != nil {
			conn.Close(err)
			return
		}

		conn.logger.Debug("receive data")

		data, err := io.ReadAll(r)

		if err != nil {
			conn.Close(err)
			return
		}

		switch t {
		case websocket.BinaryMessage, websocket.TextMessage:
			conn.readPacket(data)
		}

	}
}

func (conn *wsConn) readPacket(data []byte) error {
	packet, err := conn.p.UnpackBytes(conn.qctx, data)
	if err != nil {
		return err
	}
	conn.addPacket(packet)
	return nil
}

func (conn *wsConn) addPacket(p *protocol.Packet) {
	select {
	case conn.packetCh <- p:
	default:
		conn.logger.Warn("drop packet for channel full")
	}
}

func (conn *wsConn) writing() {
	for {
		b, ok := <-conn.writeCh
		if !ok {
			return
		}

		if conn.closed() {
			return
		}

		if err := conn.conn.WriteMessage(websocket.BinaryMessage, b); err != nil {
			conn.Close(err)
			return
		}
	}
}
