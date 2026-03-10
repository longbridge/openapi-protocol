package client

import (
	"context"
	"net"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/Allenxuxu/ringbuffer"
	protocol "github.com/longbridge/openapi-protocol/go"
	_ "github.com/longbridge/openapi-protocol/go/v1"
	_ "github.com/longbridge/openapi-protocol/go/v2"
	"github.com/pkg/errors"
)

func init() {
	RegisterDialer("tcp", DialConnFunc(dialTCPConn))
}

var _ ClientConn = &tcpConn{}

func parseHostAndPort(p string) (host, port string) {
	parts := strings.SplitN(p, ":", 2)

	return parts[0], parts[1]
}

func dialTCPConn(ctx context.Context, logger protocol.Logger, uri *url.URL, handshake *protocol.Handshake, o *DialOptions) (ClientConn, error) {

	ver := handshake.Version
	p, err := protocol.GetProtocol(ver)

	if err != nil {
		return nil, err
	}

	conn, err := net.DialTimeout("tcp", uri.Host, o.Timeout)

	if err != nil {
		return nil, err
	}

	qctx := protocol.NewContext(ctx, protocol.ClientSide)
	data := handshake.Pack()
	qctx.Codec = handshake.Codec
	qctx.Platform = handshake.Platform
	qctx.Version = handshake.Version

	c := &tcpConn{
		logger:        logger,
		qctx:          qctx,
		p:             p,
		conn:          conn,
		readBuf:       ringbuffer.New(o.ReadBufferSize),
		writeCh:       make(chan []byte, o.WriteQueueSize),
		buf:           make([]byte, 0xfffff), // alloc 1m length for reading
		closeCallback: newCloseCallback(),
	}

	c.dopts = *o

	c.closeCh = make(chan struct{})

	c.communicating()

	// do handshake
	if err = c.write(data); err != nil {
		defer c.Close(err)
		return nil, err
	}

	return c, nil
}

// tcp conn
type tcpConn struct {
	*closeCallback
	closeOnce    sync.Once
	onPacketOnce sync.Once
	conn         net.Conn

	logger protocol.Logger
	qctx   *protocol.Context
	p      protocol.Protocol

	closeCh chan struct{}

	needAuth bool

	writeBuf *ringbuffer.RingBuffer
	readBuf  *ringbuffer.RingBuffer
	writeCh  chan []byte

	buf []byte

	packetCh chan *protocol.Packet

	dopts DialOptions
}

func (conn *tcpConn) NeedHandleControl() bool {
	return true
}

func (conn *tcpConn) Context() *protocol.Context {
	return conn.qctx
}

func (conn *tcpConn) Write(p *protocol.Packet, popts ...protocol.PackOption) error {
	if conn.closed() {
		return errConnClosed
	}

	data, err := conn.p.Pack(conn.qctx, p, popts...)

	if err != nil {
		return err
	}

	return conn.write(data)
}

// TODO: change to sync write?
func (conn *tcpConn) write(data []byte) error {
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

func (conn *tcpConn) OnPacket(fn func(*protocol.Packet, error)) {
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

func (conn *tcpConn) Close(err error) {
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

func (conn *tcpConn) communicating() {
	go conn.reading()
	go conn.writing()
}

func (conn *tcpConn) closed() bool {
	select {
	case <-conn.closeCh:
		return true
	default:
	}
	return false
}

func (conn *tcpConn) reading() {
	for {
		if conn.closed() {
			return
		}

		n, err := conn.conn.Read(conn.buf)

		if err != nil {
			conn.Close(err)
			return
		}

		conn.logger.Debug("got data")

		if n == 0 {
			continue
		}

		if conn.readBuf.Length() == 0 {
			buffer := ringbuffer.NewWithData(conn.buf[:n])

			if err = conn.readPacket(buffer); err != nil {
				conn.Close(err)
				return

			}

			if buffer.Length() > 0 {
				first, _ := buffer.PeekAll()
				conn.readBuf.Write(first)
			}
		} else {
			conn.readBuf.Write(conn.buf[:n])

			if err = conn.readPacket(conn.readBuf); err != nil {
				conn.Close(err)
				return
			}
		}
	}
}

func (conn *tcpConn) readPacket(buf *ringbuffer.RingBuffer) error {
	for {
		packet, done, err := conn.p.Unpack(conn.qctx, buf)
		if err != nil {
			return err
		}

		if !done {
			break
		}

		conn.addPacket(packet)
	}

	return nil
}

func (conn *tcpConn) addPacket(p *protocol.Packet) {
	select {
	case conn.packetCh <- p:
	default:
		conn.logger.Warn("drop packet for channel full")
	}
}

func (conn *tcpConn) writing() {
	buf := ringbuffer.New(4096)

	t := time.NewTicker(time.Microsecond * 500)

	for {
		if conn.closed() {
			return
		}

		select {
		case b, ok := <-conn.writeCh:
			if !ok {
				return
			}

			if buf.Length() != 0 {
				f, e := buf.PeekAll()
				buf.Reset()

				nb := make([]byte, len(f)+len(e)+len(b))

				copy(nb, f)
				copy(nb[len(f):], e)
				copy(nb[len(f)+len(e):], b)
				b = nb
			}

			if n, err := conn.conn.Write(b); err != nil {
				conn.Close(err)
				return
			} else if n < len(b) {
				buf.Write(b[n:])
			}
		case <-t.C:
			if !buf.IsEmpty() {
				f, e := buf.PeekAll()
				buf.Reset()

				b := make([]byte, len(f)+len(e))
				copy(b, f)
				copy(b[len(f):], e)

				if n, err := conn.conn.Write(b); err != nil {
					conn.Close(err)
					return
				} else if n < len(b) {
					buf.Write(b[n:])
				}
			}
		}
	}
}
