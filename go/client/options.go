package client

import (
	"context"
	"time"

	protocol "github.com/longbridge/openapi-protocol/go"
)

var (
	defaultDialTimeout      = time.Second * 5
	defaultAuthTimeout      = time.Second * 10
	defaultKeepalive        = time.Second * 60
	defaultKeepaliveTimeout = defaultKeepalive * 2
	defaultWriteQueueSize   = 16
	defaultReadBufferSize   = 4096
	defaultReadQueueSize    = 16
	defaultMinGzipSize      = 1024

	defaultRequestTimeout = time.Second * 10
)

func newDialOptions(opts ...DialOption) *DialOptions {
	o := &DialOptions{
		Timeout:          defaultDialTimeout,
		AuthTimeout:      defaultAuthTimeout,
		KeepaliveTimeout: defaultKeepaliveTimeout,
		Keepalive:        defaultKeepalive,
		ReadBufferSize:   defaultReadBufferSize,
		ReadQueueSize:    defaultReadQueueSize,
		WriteQueueSize:   defaultWriteQueueSize,
		MinGzipSize:      defaultMinGzipSize,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// DialOption is func used to set DialOptions
type DialOption func(*DialOptions)

// DialOptions are config for dial
type DialOptions struct {
	AuthTokenGetter  func() (string, error)
	AuthTimeout      time.Duration
	Timeout          time.Duration
	Keepalive        time.Duration
	KeepaliveTimeout time.Duration
	WriteQueueSize   int
	ReadQueueSize    int
	ReadBufferSize   int
	MinGzipSize      int
	MaxReconnect     int
	ProxyFor         string
}

// ReadBufferSize set read buffer size, unit: KB
func ReadBufferSize(i int) DialOption {
	return func(o *DialOptions) {
		if i > 0 {
			o.ReadBufferSize = i << 10
		}
	}
}

// ReadQueueSize set read packet queue size
func ReadQueueSize(i int) DialOption {
	return func(o *DialOptions) {
		if i > 0 {
			o.ReadQueueSize = i
		}
	}
}

// WriteBufferSize set write buffer size, unit: KB
func WriteQueueSize(i int) DialOption {
	return func(o *DialOptions) {
		if i > 0 {
			o.WriteQueueSize = i
		}
	}
}

// DialTimeout set timeout for dialing
func DialTimeout(d time.Duration) DialOption {
	return func(o *DialOptions) {
		if d > 0 {
			o.Timeout = d
		}
	}
}

// Keepalive set heartbeat interval
func Keepalive(d time.Duration) DialOption {
	return func(o *DialOptions) {
		if d > 0 {
			o.Keepalive = d
		}
	}
}

// Keepalive set hearbeat timeout
func KeepaliveTimeout(d time.Duration) DialOption {
	return func(o *DialOptions) {
		if d > 0 {
			o.KeepaliveTimeout = d
		}
	}
}

// AuthTimeout set auth timeout
func AuthTimeout(d time.Duration) DialOption {
	return func(o *DialOptions) {
		if d > 0 {
			o.AuthTimeout = d
		}
	}
}

// MinGzipSize set gzip compress threshold
func MinGzipSize(i int) DialOption {
	return func(o *DialOptions) {
		if i > 0 {
			o.MinGzipSize = i
		}
	}
}

// MaxReconnect set max reconnect count
// Default is unlimited time until session expired
func MaxReconnect(i int) DialOption {
	return func(o *DialOptions) {
		if i > 0 {
			o.MaxReconnect = i
		}
	}
}

// WithAuthTokenGetter set AuthToken getter
func WithAuthTokenGetter(f func() (string, error)) DialOption {
	return func(o *DialOptions) {
		o.AuthTokenGetter = f
	}
}

// RequestOption is func to set RequestOptions
type RequestOption func(*RequestOptions)

// RequestOptions is request config
type RequestOptions struct {
	timeout time.Duration
}

func newRequestOptions(opts ...RequestOption) *RequestOptions {
	o := &RequestOptions{
		timeout: defaultRequestTimeout,
	}

	for _, opt := range opts {
		opt(o)
	}

	return o
}

// RequestTimeout set request timeout
func RequestTimeout(d time.Duration) RequestOption {
	return func(o *RequestOptions) {
		if d > 0 {
			o.timeout = d
		}
	}
}

// ClientOption is func to set Client Context and Logger
type ClientOption func(*client)

// WithContext set context of client
func WithContext(ctx context.Context) ClientOption {
	return func(c *client) {
		c.Context = ctx
	}
}

// WithLogger set Logger of client
func WithLogger(l protocol.Logger) ClientOption {
	return func(c *client) {
		c.Logger = l
	}
}

// WithConnectMetadata set connect metadata
func WithConnectMetadata(m map[string]string) ClientOption {
	return func(c *client) {
		c.connectMetadata = m
	}
}
