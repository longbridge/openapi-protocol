package v2

import (
	"context"
	"testing"

	"github.com/Allenxuxu/ringbuffer"
	"github.com/stretchr/testify/assert"

	protocol "github.com/longbridge/openapi-protocol/go"
	v1 "github.com/longbridge/openapi-protocol/go/v1"
)

func TestHeader_Pack(t *testing.T) {
	cases := []struct {
		label  string
		header Header
		bytes  []byte
		err    error
	}{
		{
			label: "pack request should ok",
			header: Header{
				MetadataLength: 10,
				Header: v1.Header{
					Type:       uint8(v1.RequestPacket),
					Verify:     0,
					Gzip:       0,
					Reserve:    0,
					CmdCode:    1,
					RequestId:  1,
					Timeout:    255,
					BodyLength: 7,
				},
			},

			bytes: []byte{0b00000001, 0b00000001, 0, 0, 0, 1, 0, 0xff, 0, 10, 0, 0, 7},
		},
		{
			label: "pack response should ok",
			header: Header{
				MetadataLength: 10,
				Header: v1.Header{
					Type:       uint8(v1.ResponsePacket),
					Verify:     1,
					Gzip:       1,
					Reserve:    0,
					CmdCode:    1,
					RequestId:  1,
					StatusCode: 8,
					BodyLength: 7,
				},
			},
			bytes: []byte{0b00110010, 1, 0, 0, 0, 1, 8, 0, 10, 0, 0, 7},
		},
		{
			label: "pack push should ok",
			header: Header{
				Header: v1.Header{
					Type:       uint8(v1.PushPacket),
					Verify:     0,
					Gzip:       1,
					Reserve:    3,
					CmdCode:    1,
					BodyLength: 7,
				},
			},
			bytes: []byte{0b11100011, 1, 0, 0, 0, 0, 7},
		},
		{
			label: "invalid packet type",
			header: Header{
				MetadataLength: 0,
				Header: v1.Header{
					Type: 4,
				},
			},
			err: v1.ErrUnknowPacket,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			data, err := c.header.Pack()

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
			} else {
				assert.Nil(t, err, "pack should success")
			}

			switch v1.PacketType(c.header.Type) {
			case v1.RequestPacket:
				assert.Equal(t, RequestHeaderLen, len(data))
			case v1.ResponsePacket:
				assert.Equal(t, ResponseHeaderLen, len(data))
			case v1.PushPacket:
				assert.Equal(t, PushHeaderLen, len(data))
			}

			assert.Equal(t, c.bytes, data)
		})
	}
}

func TestHeader_Unpack(t *testing.T) {
	cases := []struct {
		ctx    *protocol.Context
		err    error
		label  string
		header Header
		done   bool
		bytes  []byte
	}{
		{
			label: "unpack request shoud ok",
			header: Header{
				MetadataLength: 10,
				Header: v1.Header{
					Type:        uint8(v1.RequestPacket),
					Verify:      0,
					Gzip:        0,
					Reserve:     0,
					CmdCode:     1,
					RequestId:   1,
					Timeout:     255,
					BodyLength:  7,
					IsUnpacked:  true,
					BeginUnpack: true,
				},
			},
			bytes: []byte{0b00000001, 0b00000001, 0, 0, 0, 1, 0, 0xff, 0, 10, 0, 0, 7},
			ctx: &protocol.Context{
				Context: context.Background(),
			},
			done: true,
		},
		{
			label: "unpack response should ok",
			header: Header{
				Header: v1.Header{
					Type:        uint8(v1.ResponsePacket),
					Verify:      1,
					Gzip:        1,
					Reserve:     0,
					CmdCode:     1,
					RequestId:   1,
					StatusCode:  8,
					BodyLength:  7,
					BeginUnpack: true,
					IsUnpacked:  true,
				},
			},
			bytes: []byte{0b00110010, 1, 0, 0, 0, 1, 8, 0, 0, 0, 0, 7},
			ctx: &protocol.Context{
				Context: context.Background(),
			},
			done: true,
		},
		{
			label: "unpack push should ok",
			header: Header{
				Header: v1.Header{
					Type:        uint8(v1.PushPacket),
					Verify:      0,
					Gzip:        1,
					Reserve:     3,
					CmdCode:     1,
					BodyLength:  7,
					BeginUnpack: true,
					IsUnpacked:  true,
				},
				MetadataLength: 11,
			},
			bytes: []byte{0b11100011, 1, 0, 11, 0, 0, 7},
			done:  true,
		},
		{
			label: "invalid packet type",
			err:   v1.ErrUnknowPacket,
			bytes: []byte{0b11101100, 1, 0, 0, 7},
		},
		{
			label: "unpack unfinish",
			header: Header{
				Header: v1.Header{
					Type:        uint8(v1.PushPacket),
					Verify:      0,
					Gzip:        1,
					Reserve:     3,
					BeginUnpack: true,
					IsUnpacked:  false,
				},
			},
			bytes: []byte{0b11100011, 1, 0},
			done:  false,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			header := &Header{}
			buf := ringbuffer.New(1024)

			n, e := buf.Write(c.bytes)

			assert.Nil(t, e, "write ring buffer should ok")

			assert.Equal(t, len(c.bytes), n)

			ok, err := header.Unpack(c.ctx, buf)

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
			} else {
				assert.Nil(t, err)
			}
			assert.Equal(t, c.done, ok)

			if !ok {
				return
			}

			assert.Equal(t, c.header, *header)
		})
	}
}
func TestHeader_UnpackBytes(t *testing.T) {
	cases := []struct {
		label  string
		ctx    *protocol.Context
		err    error
		bytes  []byte
		body   []byte
		header Header
	}{
		{
			label: "unpack request shoud ok",
			header: Header{
				MetadataLength: 10,
				Header: v1.Header{
					Type:       uint8(v1.RequestPacket),
					Verify:     0,
					Gzip:       0,
					Reserve:    0,
					CmdCode:    1,
					RequestId:  1,
					Timeout:    255,
					BodyLength: 7,
				},
			},
			bytes: []byte{0b00000001, 0b00000001, 0, 0, 0, 1, 0, 0xff, 0, 10, 0, 0, 7, 1, 2, 3},
			body:  []byte{1, 2, 3},
			ctx: &protocol.Context{
				Context: context.Background(),
			},
		},
		{
			label: "unpack response should ok",
			header: Header{
				Header: v1.Header{
					Type:       uint8(v1.ResponsePacket),
					Verify:     1,
					Gzip:       1,
					Reserve:    0,
					CmdCode:    1,
					RequestId:  1,
					StatusCode: 8,
					BodyLength: 7,
				},
				MetadataLength: 257,
			},
			bytes: []byte{0b00110010, 1, 0, 0, 0, 1, 8, 1, 1, 0, 0, 7, 5, 6, 7},
			body:  []byte{5, 6, 7},
			ctx: &protocol.Context{
				Context: context.Background(),
			},
		},
		{
			label: "unpack push should ok",
			header: Header{
				Header: v1.Header{
					Type:       uint8(v1.PushPacket),
					Verify:     0,
					Gzip:       1,
					Reserve:    3,
					CmdCode:    1,
					BodyLength: 7,
				},
			},
			bytes: []byte{0b11100011, 1, 0, 0, 0, 0, 7, 1, 1, 1},
			body:  []byte{1, 1, 1},
		},
		{
			label: "invalid packet type",
			err:   v1.ErrUnknowPacket,
			bytes: []byte{0b11101100, 1, 0, 0, 7},
		},
		{
			label: "unpack unfinish",
			header: Header{
				Header: v1.Header{
					Type:    uint8(v1.PushPacket),
					Verify:  0,
					Gzip:    1,
					Reserve: 3,
				},
			},
			bytes: []byte{0b11100011, 1, 0},
			err:   v1.ErrInvalidFrame,
		},
	}

	for _, c := range cases {
		c := c
		t.Run(c.label, func(t *testing.T) {
			header := &Header{}

			body, err := header.UnpackBytes(c.ctx, c.bytes)

			if c.err != nil {
				assert.EqualError(t, err, c.err.Error())
				return
			} else {
				assert.Nil(t, err)
			}

			assert.Equal(t, c.header, *header)
			assert.Equal(t, c.body, body)
		})
	}
}

func TestHeader_Unpack_WaitData(t *testing.T) {
	bytes := []byte{0b00100010, 1, 0, 0, 0, 1, 8}
	header := Header{
		MetadataLength: 10,
		Header: v1.Header{
			Type:        uint8(v1.ResponsePacket),
			Verify:      0,
			Gzip:        1,
			Reserve:     0,
			CmdCode:     1,
			RequestId:   1,
			StatusCode:  8,
			BodyLength:  7,
			BeginUnpack: true,
			IsUnpacked:  true,
		},
	}

	buf := ringbuffer.New(1024)
	_, err := buf.Write(bytes)
	assert.Nil(t, err)

	h := &Header{}

	ctx := &protocol.Context{
		Context: context.Background(),
	}

	done, err := h.Unpack(ctx, buf)
	assert.False(t, done)
	assert.Nil(t, err)

	_, err = buf.Write([]byte{0, 10})
	assert.Nil(t, err)

	_, err = buf.Write([]byte{0, 0, 7})
	assert.Nil(t, err)

	done, err = h.Unpack(ctx, buf)
	assert.True(t, done)
	assert.Nil(t, err)

	assert.Equal(t, header, *h)
}
