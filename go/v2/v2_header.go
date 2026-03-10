package v2

import (
	"encoding/binary"
	"sync"

	"github.com/Allenxuxu/ringbuffer"

	protocol "github.com/longbridge/openapi-protocol/go"
	v1 "github.com/longbridge/openapi-protocol/go/v1"
)

const MaxMetadataLength = 1<<16 - 1

const (
	RequestHeaderLen  = v1.RequestHeaderLen + 2
	ResponseHeaderLen = v1.ResponseHeaderLen + 2
	PushHeaderLen     = v1.PushHeaderLen + 2
)

type Header struct {
	MetadataLength uint16
	v1.Header
}

func (h *Header) UnpackBytes(ctx *protocol.Context, frame []byte) (body []byte, err error) {
	if len(frame) == 0 {
		err = v1.ErrInvalidFrame
		return
	}

	var idx int

	b := frame[idx]
	idx++

	h.Type = v1.HeaderTypeMask & b
	h.Verify = b >> 4 & 0x1
	h.Gzip = b >> 5 & 0x1
	h.Reserve = b >> 6 & 0x3

	if h.IsUnknownPacket() {
		err = v1.ErrUnknowPacket
		return
	}

	var remainLen int

	t := v1.PacketType(h.Type)

	switch t {
	case v1.RequestPacket:
		remainLen = RequestHeaderLen - 1
	case v1.ResponsePacket:
		remainLen = ResponseHeaderLen - 1
	default:
		remainLen = PushHeaderLen - 1
	}

	if len(frame) < remainLen+1 {
		err = v1.ErrInvalidFrame
		return
	}

	h.CmdCode = frame[idx]
	idx++

	if t == v1.RequestPacket || t == v1.ResponsePacket {
		h.RequestId = binary.BigEndian.Uint32(frame[idx : idx+4])
		idx = idx + 4
	}

	if t == v1.RequestPacket {
		h.Timeout = binary.BigEndian.Uint16(frame[idx : idx+2])
		idx = idx + 2
	}

	if t == v1.ResponsePacket {
		h.StatusCode = frame[idx]
		idx = idx + 1
	}

	h.MetadataLength = binary.BigEndian.Uint16(frame[idx : idx+2])
	idx = idx + 2

	fb := frame[idx]
	sb := frame[idx+1]
	tb := frame[idx+2]
	idx = idx + 3

	h.BodyLength = uint32(fb)<<16 | uint32(sb)<<8 | uint32(tb)

	return frame[idx:], nil
}

func (h Header) Pack() ([]byte, error) {
	if h.IsUnknownPacket() {
		return nil, v1.ErrUnknowPacket
	}

	if h.BodyLength > v1.MaxBodyLength {
		return nil, v1.ErrBodyLenHitLimit
	}

	len := h.length()

	data := make([]byte, len)

	var idx int

	// put type:4,verify:1,gzip:1,reserve:2
	b := (h.Type & 0xf) | ((h.Verify & 0x1) << 4) | ((h.Gzip & 0x1) << 5) | ((h.Reserve & 0x3) << 6)

	data[idx] = b
	idx++

	// put cmd_code:8
	data[idx] = h.CmdCode
	idx++

	t := v1.PacketType(h.Type)

	if t == v1.RequestPacket || t == v1.ResponsePacket {
		// put request_id:32
		binary.BigEndian.PutUint32(data[idx:idx+4], h.RequestId)
		idx = idx + 4

		if t == v1.RequestPacket {
			// put timeout:16
			binary.BigEndian.PutUint16(data[idx:idx+2], h.Timeout)
			idx = idx + 2
		}

		if t == v1.ResponsePacket {
			// put status_code:8
			data[idx] = h.StatusCode
			idx = idx + 1
		}
	}

	// metedata_len:16
	binary.BigEndian.PutUint16(data[idx:idx+2], h.MetadataLength)
	idx = idx + 2

	// put body_len:24 (BigEndian)
	data[idx] = uint8(h.BodyLength >> 16)
	data[idx+1] = uint8(h.BodyLength >> 8)
	data[idx+2] = uint8(h.BodyLength)

	return data, nil

}

func (h *Header) Unpack(ctx *protocol.Context, buffer *ringbuffer.RingBuffer) (done bool, err error) {
	if buffer.Length() == 0 {
		return
	}

	// done
	if h.IsUnpacked {
		done = true
		return
	}

	if !h.BeginUnpack {
		b := buffer.PeekUint8()
		h.BeginUnpack = true

		h.Type = v1.HeaderTypeMask & b
		h.Verify = b >> 4 & 0x1
		h.Gzip = b >> 5 & 0x1
		h.Reserve = b >> 6 & 0x3

		buffer.Retrieve(1)
	}

	if h.IsUnknownPacket() {
		err = v1.ErrUnknowPacket
		return
	}

	var remainLen int

	t := v1.PacketType(h.Type)

	switch t {
	case v1.RequestPacket:
		remainLen = RequestHeaderLen - 1
	case v1.ResponsePacket:
		remainLen = ResponseHeaderLen - 1
	default:
		remainLen = PushHeaderLen - 1
	}

	// waiting data
	if buffer.Length() < remainLen {
		return
	}

	h.IsUnpacked = true
	done = true

	h.CmdCode = buffer.PeekUint8()
	buffer.Retrieve(1)

	if t == v1.RequestPacket || t == v1.ResponsePacket {
		h.RequestId = buffer.PeekUint32()
		buffer.Retrieve(4)
	}

	if t == v1.RequestPacket {
		h.Timeout = buffer.PeekUint16()
		buffer.Retrieve(2)
	}

	if t == v1.ResponsePacket {
		h.StatusCode = buffer.PeekUint8()
		buffer.Retrieve(1)
	}

	h.MetadataLength = buffer.PeekUint16()
	buffer.Retrieve(2)

	f, e := buffer.Peek(3)
	buffer.Retrieve(3)

	// first byte, second byte, third byte
	var fb, sb, tb byte

	switch len(f) {
	case 0:
		fb = e[0]
		sb = e[1]
		tb = e[2]
	case 1:
		fb = f[0]
		sb = e[0]
		tb = e[1]
	case 2:
		fb = f[0]
		sb = f[1]
		tb = e[0]
	default:
		fb = f[0]
		sb = f[1]
		tb = f[2]
	}

	h.BodyLength = uint32(fb)<<16 | uint32(sb)<<8 | uint32(tb)

	return
}

func (h Header) length() int {
	switch v1.PacketType(h.Type) {
	case v1.RequestPacket:
		return RequestHeaderLen
	case v1.ResponsePacket:
		return ResponseHeaderLen
	default:
		return PushHeaderLen
	}
}

var defaultHeaderPool = &headerPool{
	Pool: &sync.Pool{
		New: func() interface{} {
			return &Header{}
		},
	},
}

type headerPool struct {
	*sync.Pool
}

func (p *headerPool) Get() *Header {
	v := p.Pool.Get()
	h := v.(*Header)

	h.Type = 0
	h.Verify = 0
	h.Gzip = 0
	h.Reserve = 0
	h.CmdCode = 0
	h.Timeout = 0
	h.RequestId = 0
	h.StatusCode = 0
	h.BodyLength = 0
	h.MetadataLength = 0

	h.BeginUnpack = false
	h.IsUnpacked = false

	return h
}

func headerFromContext(ctx *protocol.Context) (h *Header) {
	v := ctx.GetHeader()

	if v == nil {
		h = defaultHeaderPool.Get()
		ctx.SetHeader(h)
	} else {
		h = v.(*Header)
	}

	return
}

func headerFromMetadata(md *protocol.Metadata) *Header {
	h := defaultHeaderPool.Get()

	if md.Gzip {
		h.Gzip = 1
	}

	if md.Verify {
		h.Verify = 1
	}

	switch md.Type {
	case protocol.RequestPacket:
		h.Type = uint8(v1.RequestPacket)
	case protocol.ResponsePacket:
		h.Type = uint8(v1.ResponsePacket)
	case protocol.PushPacket:
		h.Type = uint8(v1.PushPacket)
	}

	h.RequestId = md.RequestId
	h.CmdCode = uint8(md.CmdCode & 0xff)
	h.Timeout = md.Timeout
	h.StatusCode = md.StatusCode

	return h
}
