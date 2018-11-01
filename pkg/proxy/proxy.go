package proxy

import (
	"time"
)

// Option option
type Option func(*options)

type options struct {
	dim     int
	timeout time.Duration
}

func (opts *options) adjust() {
	if opts.dim == 0 {
		opts.dim = 512
	}

	if opts.timeout == 0 {
		opts.timeout = time.Millisecond * 200
	}
}

// WithDim with dim option
func WithDim(dim int) Option {
	return func(opts *options) {
		opts.dim = dim
	}
}

// WithSearchTimeout with timeout option
func WithSearchTimeout(timeout time.Duration) Option {
	return func(opts *options) {
		opts.timeout = timeout
	}
}

// Proxy is proxy for versiondb
type Proxy interface {
	UpdateWithIds(db uint64, extXb []float32, extXids []int64) error
	AddWithIds(newXb []float32, newXids []int64) error
	Search(xq []float32) ([]float32, []int64, error)
}
