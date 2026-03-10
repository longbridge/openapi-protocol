package protocol

import (
	"encoding/json"

	control "github.com/longbridge/openapi-protobufs/gen/go/control"
	"github.com/pkg/errors"
	"google.golang.org/protobuf/proto"
)

// PacketType is type of packet.
// We have 3 types now: reqeust, response and push.
type PacketType string

const (
	RequestPacket  PacketType = "request"
	ResponsePacket PacketType = "response"
	PushPacket     PacketType = "push"
)

func (p PacketType) String() string {
	return string(p)
}

type Packet struct {
	Metadata *Metadata
	Body     []byte
}

func (p Packet) UnmarshalMetadata(data []byte) error {
	return p.Metadata.UnmarshalValues(data)
}

func (p Packet) MarshalMetadata(max int) []byte {
	return p.Metadata.MarshalValues(max)
}

func (p Packet) SetMetadata(k, v string) {
	if p.Metadata == nil {
		return
	}

	p.Metadata.Set(k, v)
}

func (p Packet) SetMetadataPairs(pairs ...KVPair) {
	for _, pair := range pairs {
		p.SetMetadata(pair.Key, pair.Val)
	}
}

func (p Packet) GetMetadata(k string) string {
	return p.Metadata.Get(k)
}

func (p Packet) IsControl() bool {
	return IsControl(p.Metadata.CmdCode)
}

func (p Packet) IsAuth() bool {
	return IsAuth(p.Metadata.CmdCode)
}

func (p Packet) IsReconnect() bool {
	return IsReconnect(p.Metadata.CmdCode)
}

func (p Packet) IsClose() bool {
	return IsClose(p.Metadata.CmdCode)
}

func (p Packet) IsPing() bool {
	return IsHeartbeat(p.Metadata.CmdCode) && p.Metadata.Type == RequestPacket
}

func (p Packet) IsPong() bool {
	return IsHeartbeat(p.Metadata.CmdCode) && p.Metadata.Type == ResponsePacket
}

func (p Packet) StatusCode() uint8 {
	return p.Metadata.StatusCode
}

func (p Packet) CMD() uint32 {
	return p.Metadata.CmdCode
}

func (p Packet) Err() error {
	if p.Metadata.Type != ResponsePacket {
		return nil
	}

	if p.Metadata.StatusCode == StatusSuccess {
		return nil
	}

	var e control.Error

	if err := p.Unmarshal(&e); err != nil {
		e = control.Error{
			Code: 500,
			Msg:  "unknown error, cant unmarshal body",
		}
	}

	return NewError(p.Metadata.StatusCode, e.GetCode(), e.GetMsg())
}

func (p Packet) Unmarshal(v interface{}) error {
	if p.Metadata.Codec == CodecJSON {
		return json.Unmarshal(p.Body, v)
	}

	if p.Metadata.Codec == CodecProtobuf {
		m, ok := v.(proto.Message)

		if !ok {
			return errors.New("must be proto.Message")
		}

		return proto.Unmarshal(p.Body, m)
	}

	return errors.New("unknown codec type")
}

type PacketOption func(*Metadata)

func WithVerify(nonce uint64, signature []byte) PacketOption {
	return func(md *Metadata) {
		md.Nonce = nonce
		md.Signature = signature
		md.Verify = true
	}
}

func WithRequestId(id uint32) PacketOption {
	return func(md *Metadata) {
		md.RequestId = id
	}
}

func WithStatusCode(code uint8) PacketOption {
	return func(md *Metadata) {
		md.StatusCode = code
	}
}

func NewPush(ctx *Context, cmd uint32, body interface{}, opts ...PacketOption) (Packet, error) {
	return NewPacket(ctx, PushPacket, cmd, body, opts...)
}

func MustNewPush(ctx *Context, cmd uint32, body interface{}, opts ...PacketOption) Packet {
	p, e := NewPacket(ctx, PushPacket, cmd, body, opts...)

	if e != nil {
		panic(e)
	}

	return p
}

func MustNewRequest(ctx *Context, cmd uint32, body interface{}, opts ...PacketOption) Packet {
	opts = append(opts, WithRequestId(ctx.NextReqId()))

	p, e := NewPacket(ctx, RequestPacket, cmd, body, opts...)

	if e != nil {
		panic(e)
	}

	return p
}

func NewRequest(ctx *Context, cmd uint32, body interface{}, opts ...PacketOption) (Packet, error) {
	opts = append(opts, WithRequestId(ctx.NextReqId()))

	return NewPacket(ctx, RequestPacket, cmd, body, opts...)
}

func MustNewResponse(ctx *Context, cmd uint32, code uint8, body interface{}, opts ...PacketOption) Packet {
	opts = append(opts, WithStatusCode(code))
	p, e := NewPacket(ctx, ResponsePacket, cmd, body, opts...)
	if e != nil {
		panic(e)
	}
	return p
}

func NewResponse(ctx *Context, cmd uint32, sc uint8, body interface{}, opts ...PacketOption) (Packet, error) {
	opts = append(opts, WithStatusCode(sc))
	return NewPacket(ctx, ResponsePacket, cmd, body, opts...)
}

func NewPacket(ctx *Context, t PacketType, cmd uint32, body interface{}, opts ...PacketOption) (Packet, error) {
	md := &Metadata{
		Type:    t,
		Codec:   ctx.Codec,
		CmdCode: cmd,
		Values:  make(map[string]string, 0),
	}

	for _, opt := range opts {
		opt(md)
	}

	var (
		data []byte
		err  error
	)

	if body != nil {
		switch v := body.(type) {
		case []byte:
			data = v
		default:
			if data, err = marshal(ctx.Codec, body); err != nil {
				return Packet{}, err
			}
		}
	}

	return Packet{
		Metadata: md,
		Body:     data,
	}, nil
}

func marshal(c CodecType, v interface{}) (data []byte, err error) {
	switch c {
	case CodecProtobuf:
		m, ok := v.(proto.Message)

		if !ok {
			return nil, errors.New("marshal target should imply proto.Message interface")
		}
		data, err = proto.Marshal(m)
	case CodecJSON:
		data, err = json.Marshal(v)
	default:
		return nil, errors.Errorf("invalid codec type: %s", c.String())
	}

	return
}

func IsControl(cmd uint32) bool {
	return cmd <= uint32(control.Command_CMD_RECONNECT)
}

func IsClose(cmd uint32) bool {
	return cmd == uint32(control.Command_CMD_CLOSE)
}

func IsHeartbeat(cmd uint32) bool {
	return cmd == uint32(control.Command_CMD_HEARTBEAT)
}

func IsAuth(cmd uint32) bool {
	return cmd == uint32(control.Command_CMD_AUTH)
}

func IsReconnect(cmd uint32) bool {
	return cmd == uint32(control.Command_CMD_RECONNECT)
}
