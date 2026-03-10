package v2

import (
	"encoding/binary"

	"github.com/Allenxuxu/ringbuffer"

	protocol "github.com/longbridge/openapi-protocol/go"
	"github.com/longbridge/openapi-protocol/go/gzip"
	v1 "github.com/longbridge/openapi-protocol/go/v1"
)

func init() {
	protocol.Register(2, &protocolV2{})
}

var _ (protocol.Protocol) = (*protocolV2)(nil)

type protocolV2 struct{}

func (protocolV2) Version() uint8 { return 2 }

func (p *protocolV2) UnpackBytes(ctx *protocol.Context, bs []byte) (packet *protocol.Packet, err error) {
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

	if len(data) < (int(header.BodyLength) + int(header.MetadataLength)) {
		err = v1.ErrInvalidFrame
		return
	}

	md := data[:header.MetadataLength]

	packet = &protocol.Packet{
		Metadata: header.Metadata(ctx),
		Body:     data[header.MetadataLength : header.BodyLength+uint32(header.MetadataLength)],
	}

	if err = packet.UnmarshalMetadata(md); err != nil {
		return
	}

	if header.Verify == 1 {
		idx := int(header.BodyLength) + int(header.MetadataLength)

		if len(data) < idx+v1.NonceLength+v1.SignatureLength {
			err = v1.ErrInvalidFrame
			return
		}

		packet.Metadata.Nonce = binary.BigEndian.Uint64(data[idx : idx+v1.NonceLength])
		packet.Metadata.Signature = data[idx+v1.NonceLength:]
	}

	if header.Gzip == 1 {
		if packet.Body, _, err = gzip.Decompress(packet.Body); err != nil {
			return
		}
	}

	ctx.EndUnpack()

	return
}

func (p *protocolV2) Unpack(ctx *protocol.Context, buf *ringbuffer.RingBuffer) (packet *protocol.Packet, done bool, err error) {
	ctx.BeginUnpack()

	header := headerFromContext(ctx)

	defer func() {
		// if done or err raised, release header
		if done || err != nil {
			ctx.SetHeader(nil)
			defaultHeaderPool.Put(header)
		}
	}()

	if !header.Unpacked() {
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

	len := int(header.BodyLength) + int(header.MetadataLength)

	if header.Verify == 1 {
		len = len + v1.NonceLength + v1.SignatureLength
	}

	// wait all byte ready
	if buf.Length() < len {
		return
	}

	// read metadata
	md := make([]byte, header.MetadataLength)

	if _, err = buf.Read(md); err != nil {
		return
	}

	// read body
	body := make([]byte, header.BodyLength)
	if _, err = buf.Read(body); err != nil {
		return
	}

	packet = &protocol.Packet{
		Metadata: header.Metadata(ctx),
		Body:     body,
	}

	if err = packet.UnmarshalMetadata(md); err != nil {
		return
	}

	if header.Verify == 1 {
		packet.Metadata.Nonce = buf.PeekUint64()
		buf.Retrieve(v1.NonceLength)

		s := make([]byte, v1.SignatureLength)
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

func (p *protocolV2) Pack(ctx *protocol.Context, packet *protocol.Packet, opts ...protocol.PackOption) ([]byte, error) {
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

	if bl > v1.MaxBodyLength {
		return nil, v1.ErrBodyLenHitLimit
	}

	h := headerFromMetadata(packet.Metadata)
	defer func() {
		defaultHeaderPool.Put(h)
	}()

	md := packet.MarshalMetadata(MaxMetadataLength)
	h.MetadataLength = uint16(len(md))

	h.BodyLength = uint32(bl)

	// TODO: optmize using []byte pool
	hd, err := h.Pack()

	if err != nil {
		return nil, err
	}

	var hl int

	switch v1.PacketType(h.Type) {
	case v1.RequestPacket:
		hl = RequestHeaderLen
	case v1.ResponsePacket:
		hl = ResponseHeaderLen
	case v1.PushPacket:
		hl = PushHeaderLen
	}

	l := hl + bl + len(md)

	if packet.Metadata.Verify {
		l = l + v1.NonceLength + v1.SignatureLength
	}

	data := make([]byte, l)

	copy(data, hd)
	copy(data[len(hd):], md)
	copy(data[len(hd)+len(md):], packet.Body)

	if packet.Metadata.Verify {
		binary.BigEndian.PutUint64(data[hl+bl:hl+bl+v1.NonceLength], packet.Metadata.Nonce)
		copy(data[hl+bl+v1.NonceLength:], packet.Metadata.Signature)
	}

	return data, nil
}
