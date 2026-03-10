package v1

import (
	"encoding/binary"

	"github.com/Allenxuxu/ringbuffer"

	protocol "github.com/longbridge/openapi-protocol/go"
	"github.com/longbridge/openapi-protocol/go/gzip"
)

func init() {
	protocol.Register(1, &protocolV1{})
}

const (
	NonceLength     = 8
	SignatureLength = 16
)

var _ protocol.Protocol = &protocolV1{}

type protocolV1 struct {
}

func (p *protocolV1) Version() uint8 {
	return 1
}

func (p *protocolV1) UnpackBytes(ctx *protocol.Context, bs []byte) (packet *protocol.Packet, err error) {
	ctx.BeginUnpack()
	header := headerFromContext(ctx)

	defer func() {
		ctx.SetHeader(nil)
		defaultHeaderPool.Put(header)
	}()

	data, e := header.UnpackBytes(ctx, bs)

	if e != nil {
		err = e
		return
	}

	if len(data) < int(header.BodyLength) {
		err = ErrInvalidFrame
		return
	}

	packet = &protocol.Packet{
		Metadata: header.Metadata(ctx),
		Body:     data[:header.BodyLength],
	}

	if header.Verify == 1 {
		idx := int(header.BodyLength)

		if len(data) < idx+NonceLength+SignatureLength {
			err = ErrInvalidFrame
			return
		}

		packet.Metadata.Nonce = binary.BigEndian.Uint64(data[idx : idx+NonceLength])
		packet.Metadata.Signature = data[idx+NonceLength:]
	}

	if header.Gzip == 1 {
		if packet.Body, _, err = gzip.Decompress(packet.Body); err != nil {
			return
		}
	}

	ctx.EndUnpack()

	return
}

func (p *protocolV1) Unpack(ctx *protocol.Context, buf *ringbuffer.RingBuffer) (packet *protocol.Packet, done bool, err error) {
	ctx.BeginUnpack()

	header := headerFromContext(ctx)

	defer func() {
		// if done or err raised, release header
		if done || err != nil {
			ctx.SetHeader(nil)
			defaultHeaderPool.Put(header)
		}
	}()

	if !header.IsUnpacked {
		ok, e := header.Unpack(ctx, buf)

		if e != nil {
			err = e
			return
		}

		// waiting header data
		if !ok {
			return
		}
	}

	len := int(header.BodyLength)

	if header.Verify == 1 {
		len = len + NonceLength + SignatureLength
	}

	// wait all byte ready
	if buf.Length() < len {
		return
	}

	body := make([]byte, header.BodyLength)
	if _, err = buf.Read(body); err != nil {
		return
	}

	packet = &protocol.Packet{
		Metadata: header.Metadata(ctx),
		Body:     body,
	}

	if header.Verify == 1 {
		packet.Metadata.Nonce = buf.PeekUint64()
		buf.Retrieve(NonceLength)

		s := make([]byte, SignatureLength)
		if _, err = buf.Read(s); err != nil {
			return
		}
		packet.Metadata.Signature = s
	}

	if header.Gzip == 1 {
		if packet.Body, _, err = gzip.Decompress(packet.Body); err != nil {
			return
		}
	}

	// unpack is done
	done = true

	ctx.EndUnpack()
	return

}

func (p *protocolV1) Pack(ctx *protocol.Context, packet *protocol.Packet, opts ...protocol.PackOption) ([]byte, error) {
	o := protocol.NewPackOptions(opts...)

	bl := len(packet.Body)

	if o.MinGzipSize != 0 && bl >= o.MinGzipSize {
		var err error
		if packet.Body, err = gzip.Compress(packet.Body); err != nil {
			return nil, err
		}

		bl = len(packet.Body)
		packet.Metadata.Gzip = true
	}

	if bl > MaxBodyLength {
		return nil, ErrBodyLenHitLimit
	}

	h := headerFromMetadata(packet.Metadata)
	defer func() {
		defaultHeaderPool.Put(h)
	}()

	h.BodyLength = uint32(bl)

	// TODO: optmize using []byte pool
	hd, err := h.Pack()

	if err != nil {
		return nil, err
	}

	var hl int

	switch PacketType(h.Type) {
	case RequestPacket:
		hl = RequestHeaderLen
	case ResponsePacket:
		hl = ResponseHeaderLen
	case PushPacket:
		hl = PushHeaderLen
	}

	l := hl + bl

	if packet.Metadata.Verify {
		l = l + NonceLength + SignatureLength
	}

	data := make([]byte, l)

	copy(data, hd)
	copy(data[len(hd):], packet.Body)

	if packet.Metadata.Verify {
		binary.BigEndian.PutUint64(data[hl+bl:hl+bl+NonceLength], packet.Metadata.Nonce)
		copy(data[hl+bl+NonceLength:], packet.Metadata.Signature)
	}

	return data, nil
}
