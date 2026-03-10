package v1

import (
	"context"
	"testing"

	"github.com/Allenxuxu/ringbuffer"
	"github.com/stretchr/testify/assert"

	protocol "github.com/longbridge/openapi-protocol/go"
)

var v1 = &protocolV1{}

func TestProtocolV1_LogUpack(t *testing.T) {
	bytes := []byte{33, 125, 52, 127, 60, 125, 102, 40, 1, 63, 126}
	buf := ringbuffer.New(1024)
	buf.Write(bytes)

	ctx := protocol.NewContext(context.Background(), protocol.ServerSide)
	p, done, err := v1.Unpack(ctx, buf)

	t.Log(p)
	t.Log(done)
	t.Log(err)
}

func TestProtocolV1_Unpack_WaitData(t *testing.T) {
	packet := &protocol.Packet{
		Metadata: &protocol.Metadata{
			Type:      protocol.RequestPacket,
			Gzip:      false,
			Verify:    false,
			Timeout:   255,
			RequestId: 1,
			CmdCode:   1,
		},
		Body: []byte("hello world"),
	}

	bytes := []byte{0b00000001, 1, 0, 0, 0, 1, 0, 255, 0, 0, 11}

	ctx := protocol.NewContext(context.Background(), protocol.ServerSide)

	buf := ringbuffer.New(1024)

	_, err := buf.Write(bytes)
	assert.Nil(t, err)

	p, done, err := v1.Unpack(ctx, buf)

	assert.Nil(t, err)
	assert.False(t, done)
	assert.Nil(t, p)

	_, err = buf.Write([]byte("hello world111"))

	assert.Nil(t, err)

	p, done, err = v1.Unpack(ctx, buf)

	assert.Nil(t, err)
	assert.True(t, done)

	assert.Equal(t, *p.Metadata, *packet.Metadata)
	assert.Equal(t, p.Body, packet.Body)

}

func TestProtocolV1_Unpack(t *testing.T) {
	cases := []struct {
		label  string
		ctx    *protocol.Context
		packet *protocol.Packet
		bytes  []byte
		err    error
		done   bool
	}{
		{
			label: "request packet should unpack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.RequestPacket,
					Gzip:      false,
					Verify:    false,
					Timeout:   255,
					RequestId: 1,
					CmdCode:   1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000001, 1, 0, 0, 0, 1, 0, 255, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
			done:  true,
		},
		{
			label: "response packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:       protocol.ResponsePacket,
					Gzip:       false,
					Verify:     false,
					StatusCode: 1,
					RequestId:  1,
					CmdCode:    1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000010, 1, 0, 0, 0, 1, 1, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
			done:  true,
		},
		{
			label: "push packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:    protocol.PushPacket,
					Gzip:    false,
					Verify:  false,
					CmdCode: 3,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
			done:  true,
		},
		{
			label: "with signature should pack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.PushPacket,
					Gzip:      false,
					Verify:    true,
					CmdCode:   3,
					Nonce:     1,
					Signature: []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00010011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', 0, 0, 0, 0, 0, 0, 0, 1, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
			done:  true,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			buf := ringbuffer.New(1024)

			_, err := buf.Write(c.bytes)

			assert.Nil(t, err)

			p, done, err := v1.Unpack(c.ctx, buf)

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, c.done, done)

			if !done {
				return
			}

			assert.Equal(t, *c.packet.Metadata, *p.Metadata)
			assert.Equal(t, c.packet.Body, p.Body)
		})
	}
}

func TestProtocolV1_UnpackBytes(t *testing.T) {
	cases := []struct {
		label  string
		ctx    *protocol.Context
		packet *protocol.Packet
		bytes  []byte
		err    error
	}{
		{
			label: "request packet should unpack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.RequestPacket,
					Gzip:      false,
					Verify:    false,
					Timeout:   255,
					RequestId: 1,
					CmdCode:   1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000001, 1, 0, 0, 0, 1, 0, 255, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "response packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:       protocol.ResponsePacket,
					Gzip:       false,
					Verify:     false,
					StatusCode: 1,
					RequestId:  1,
					CmdCode:    1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000010, 1, 0, 0, 0, 1, 1, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "push packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:    protocol.PushPacket,
					Gzip:    false,
					Verify:  false,
					CmdCode: 3,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "with signature should pack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.PushPacket,
					Gzip:      false,
					Verify:    true,
					CmdCode:   3,
					Nonce:     1,
					Signature: []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00010011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', 0, 0, 0, 0, 0, 0, 0, 1, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			p, err := v1.UnpackBytes(c.ctx, c.bytes)

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, *c.packet.Metadata, *p.Metadata)
			assert.Equal(t, c.packet.Body, p.Body)
		})
	}
}

func TestProtocolV1_Pack(t *testing.T) {
	cases := []struct {
		label  string
		ctx    *protocol.Context
		packet *protocol.Packet
		bytes  []byte
		err    error
	}{
		{
			label: "request packet should pack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.RequestPacket,
					Gzip:      false,
					Verify:    false,
					Timeout:   255,
					RequestId: 1,
					CmdCode:   1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000001, 1, 0, 0, 0, 1, 0, 255, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "response packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:       protocol.ResponsePacket,
					Gzip:       false,
					Verify:     false,
					StatusCode: 1,
					RequestId:  1,
					CmdCode:    1,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000010, 1, 0, 0, 0, 1, 1, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "push packet should pack ok",

			ctx: &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:    protocol.PushPacket,
					Gzip:    false,
					Verify:  false,
					CmdCode: 3,
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00000011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd'},
		},
		{
			label: "with signature should pack ok",
			ctx:   &protocol.Context{},
			packet: &protocol.Packet{
				Metadata: &protocol.Metadata{
					Type:      protocol.PushPacket,
					Gzip:      false,
					Verify:    true,
					CmdCode:   3,
					Nonce:     1,
					Signature: []byte{'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
				},
				Body: []byte("hello world"),
			},
			bytes: []byte{0b00010011, 3, 0, 0, 11, 'h', 'e', 'l', 'l', 'o', ' ', 'w', 'o', 'r', 'l', 'd', 0, 0, 0, 0, 0, 0, 0, 1, '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'},
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			data, err := v1.Pack(c.ctx, c.packet)

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, c.bytes, data)

		})
	}
}

func TestGetV1Protocol(t *testing.T) {
	_, err := protocol.GetProtocol(1)

	assert.Nil(t, err)
}
