package server

import (
	"github.com/infinivision/hyena/pkg/raftstore"
)

// Option option
type Option func(*options)

type options struct {
	raftOptions []raftstore.Option

	addr, raftAddr string
}

// WithRaftOption append a raft option
func WithRaftOption(opt raftstore.Option) Option {
	return func(opts *options) {
		opts.raftOptions = append(opts.raftOptions, opt)
	}
}

func (opts *options) adjust() {
	if opts.addr == "" {
		opts.addr = "127.0.0.1:9527"
	}

	if opts.raftAddr == "" {
		opts.addr = "127.0.0.1:9528"
	}
}
