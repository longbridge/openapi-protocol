package v1

import (
	"encoding/binary"
	"sync"

	"github.com/Allenxuxu/ringbuffer"
	"github.com/pkg/errors"

	protocol "github.com/longbridge/openapi-protocol/go"
)

var ErrUnknowPacket = errors.New("invalid packet type")
var ErrBodyLenHitLimit = errors.New("body length hit limit")
var ErrInvalidFrame = errors.New("invalid frame")

var defaultHeaderPool = newHeaderPool()

type PacketType uint8

const (
	UnkownPacket PacketType = iota
	RequestPacket
	ResponsePacket
	PushPacket
)

const (
	RequestHeaderLen  = 11
	ResponseHeaderLen = 10
	PushHeaderLen     = 5
)

const (
	HeaderTypeMask = 0xf
)

const MaxBodyLength = 1<<24 - 1

type Header struct {
	RequestId  uint32
	BodyLength uint32
	Timeout    uint16
	Type       uint8
	Verify     uint8
	Gzip       uint8
	Reserve    uint8
	CmdCode    uint8
	StatusCode uint8

	BeginUnpack bool
	IsUnpacked  bool
}

func (h Header) Metadata(ctx *protocol.Context) *protocol.Metadata {
	var t protocol.PacketType

	switch PacketType(h.Type) {
	case RequestPacket:
		t = protocol.RequestPacket
	case ResponsePacket:
		t = protocol.ResponsePacket
	case PushPacket:
		t = protocol.PushPacket
	}

	return &protocol.Metadata{
		Type:       t,
		Codec:      ctx.Codec,
		Timeout:    h.Timeout,
		CmdCode:    uint32(h.CmdCode),
		RequestId:  h.RequestId,
		StatusCode: h.StatusCode,
		Verify:     h.Verify == 1,
		Gzip:       h.Gzip == 1,
	}
}

func (h Header) IsUnknownPacket() bool {
	switch PacketType(h.Type) {
	case RequestPacket:
		return false
	case ResponsePacket:
		return false
	case PushPacket:
		return false
	default:
		return true
	}
}

func (h Header) Unpacked() bool {
	return h.IsUnpacked
}

func (h Header) length() uint8 {
	switch PacketType(h.Type) {
	case RequestPacket:
		return RequestHeaderLen
	case ResponsePacket:
		return ResponseHeaderLen
	default:
		return PushHeaderLen
	}
}

func (h Header) Pack() ([]byte, error) {
	if h.IsUnknownPacket() {
		return nil, ErrUnknowPacket
	}

	if h.BodyLength > MaxBodyLength {
		return nil, ErrBodyLenHitLimit
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

	t := PacketType(h.Type)

	if t == RequestPacket || t == ResponsePacket {
		// put request_id:32
		binary.BigEndian.PutUint32(data[idx:idx+4], h.RequestId)
		idx = idx + 4

		if t == RequestPacket {
			// put timeout:16
			binary.BigEndian.PutUint16(data[idx:idx+2], h.Timeout)
			idx = idx + 2
		}

		if t == ResponsePacket {
			// put status_code:8
			data[idx] = h.StatusCode
			idx = idx + 1
		}
	}

	// put body_len:24 (BigEndian)
	data[idx] = uint8(h.BodyLength >> 16)
	data[idx+1] = uint8(h.BodyLength >> 8)
	data[idx+2] = uint8(h.BodyLength)

	return data, nil
}

func (h *Header) UnpackBytes(ctx *protocol.Context, frame []byte) (body []byte, err error) {
	if len(frame) == 0 {
		err = ErrInvalidFrame
		return
	}

	var idx int

	b := frame[idx]
	idx++

	h.Type = HeaderTypeMask & b
	h.Verify = b >> 4 & 0x1
	h.Gzip = b >> 5 & 0x1
	h.Reserve = b >> 6 & 0x3

	if h.IsUnknownPacket() {
		err = ErrUnknowPacket
		return
	}

	var remainLen int

	t := PacketType(h.Type)

	switch t {
	case RequestPacket:
		remainLen = RequestHeaderLen - 1
	case ResponsePacket:
		remainLen = ResponseHeaderLen - 1
	default:
		remainLen = PushHeaderLen - 1
	}

	if len(frame) < remainLen+1 {
		err = ErrInvalidFrame
		return
	}

	h.CmdCode = frame[idx]
	idx++

	if t == RequestPacket || t == ResponsePacket {
		h.RequestId = binary.BigEndian.Uint32(frame[idx : idx+4])
		idx = idx + 4
	}

	if t == RequestPacket {
		h.Timeout = binary.BigEndian.Uint16(frame[idx : idx+2])
		idx = idx + 2
	}

	if t == ResponsePacket {
		h.StatusCode = frame[idx]
		idx = idx + 1
	}

	fb := frame[idx]
	sb := frame[idx+1]
	tb := frame[idx+2]
	idx = idx + 3

	h.BodyLength = uint32(fb)<<16 | uint32(sb)<<8 | uint32(tb)

	return frame[idx:], nil
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

		h.Type = HeaderTypeMask & b
		h.Verify = b >> 4 & 0x1
		h.Gzip = b >> 5 & 0x1
		h.Reserve = b >> 6 & 0x3

		buffer.Retrieve(1)
	}

	if h.IsUnknownPacket() {
		err = ErrUnknowPacket
		return
	}

	var remainLen int

	t := PacketType(h.Type)

	switch t {
	case RequestPacket:
		remainLen = RequestHeaderLen - 1
	case ResponsePacket:
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

	if t == RequestPacket || t == ResponsePacket {
		h.RequestId = buffer.PeekUint32()
		buffer.Retrieve(4)
	}

	if t == RequestPacket {
		h.Timeout = buffer.PeekUint16()
		buffer.Retrieve(2)
	}

	if t == ResponsePacket {
		h.StatusCode = buffer.PeekUint8()
		buffer.Retrieve(1)
	}

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

func newHeaderPool() *headerPool {
	p := &sync.Pool{
		New: func() interface{} {
			return &Header{}
		},
	}

	return &headerPool{
		Pool: p,
	}
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
		h.Type = uint8(RequestPacket)
	case protocol.ResponsePacket:
		h.Type = uint8(ResponsePacket)
	case protocol.PushPacket:
		h.Type = uint8(PushPacket)
	}

	h.RequestId = md.RequestId
	h.CmdCode = uint8(md.CmdCode & 0xff)
	h.Timeout = md.Timeout
	h.StatusCode = md.StatusCode

	return h
}
