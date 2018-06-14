package server

import (
	"time"

	"github.com/infinivision/hyena/pkg/raftstore"
)

// Option option
type Option func(*options)

type options struct {
	raftOptions []raftstore.Option

	addr, raftAddr string
	timeoutRead    time.Duration
}

// WithClientReadTimeout using client read timeout
func WithClientReadTimeout(value time.Duration) Option {
	return func(opts *options) {
		opts.timeoutRead = value
	}
}

// WithRaftOption append a raft option
func WithRaftOption(opt raftstore.Option) Option {
	return func(opts *options) {
		opts.raftOptions = append(opts.raftOptions, opt)
	}
}

func (opts *options) adjust() {
	if opts.timeoutRead == 0 {
		opts.timeoutRead = time.Second * 30
	}

	if opts.addr == "" {
		opts.addr = "127.0.0.1:9527"
	}

	if opts.raftAddr == "" {
		opts.addr = "127.0.0.1:9528"
	}
}
