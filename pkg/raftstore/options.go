package raftstore

import (
	"time"

	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/prophet"
)

type options struct {
	cfg *Cfg
}

func (opts *options) adjust() {
	opts.cfg.Adjust()
}

// Option raftstore option
type Option func(*options)

// WithProphetOption append a prophet option
func WithProphetOption(opt prophet.Option) Option {
	return func(opts *options) {
		opts.cfg.ProphetOptions = append(opts.cfg.ProphetOptions, opt)
	}
}

// WithProphetName set prophet name
func WithProphetName(name string) Option {
	return func(opts *options) {
		opts.cfg.ProphetName = name
	}
}

// WithProphetAddr set prophet name
func WithProphetAddr(addr string) Option {
	return func(opts *options) {
		opts.cfg.ProphetAddr = addr
	}
}

// WithStoreDataPath set raftstore storage data path
func WithStoreDataPath(path string) Option {
	return func(opts *options) {
		opts.cfg.DataPath = path
	}
}

// WithStoreOptionsPath set raftstore storage option path
func WithStoreOptionsPath(path string) Option {
	return func(opts *options) {
		opts.cfg.StoreOptionsPath = path
	}
}

// WithLabel set raftstore storage option path
func WithLabel(key, value string) Option {
	return func(opts *options) {
		opts.cfg.Lables = append(opts.cfg.Lables, meta.Label{
			Key:   key,
			Value: value,
		})
	}
}

// WithElectionTick set ElectionTick
func WithElectionTick(value int) Option {
	return func(opts *options) {
		opts.cfg.ElectionTick = value
	}
}

// WithHeartbeatTick set HeartbeatTick
func WithHeartbeatTick(value int) Option {
	return func(opts *options) {
		opts.cfg.HeartbeatTick = value
	}
}

// WithMaxSizePerMsg set MaxSizePerMsg
func WithMaxSizePerMsg(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxSizePerMsg = value
	}
}

// WithMaxInflightMsgs set MaxInflightMsgs
func WithMaxInflightMsgs(value int) Option {
	return func(opts *options) {
		opts.cfg.MaxInflightMsgs = value
	}
}

// WithMinRaftLogCount set MinRaftLogCount
func WithMinRaftLogCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MinRaftLogCount = value
	}
}

// WithMaxRaftLogCount set MaxRaftLogCount
func WithMaxRaftLogCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRaftLogCount = value
	}
}

// WithMaxRaftLogBytes set MaxRaftLogBytes
func WithMaxRaftLogBytes(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRaftLogBytes = value
	}
}

// WithMaxRaftLogLag set MaxRaftLogLag
func WithMaxRaftLogLag(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRaftLogLag = value
	}
}

// WithMaxRaftEntryBytes set MaxRaftEntryBytes
func WithMaxRaftEntryBytes(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxRaftEntryBytes = value
	}
}

// WithRaftTickDuration set RaftTickDuration
func WithRaftTickDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.RaftTickDuration = value
	}
}

// WithRaftLogCompactDuration set RaftLogCompactDuration
func WithRaftLogCompactDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.RaftLogCompactDuration = value
	}
}

// WithRaftCheckSplitDuration set RaftCheckSplitDuration
func WithRaftCheckSplitDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.RaftCheckSplitDuration = value
	}
}

// WithSyncWrite set SyncWrite
func WithSyncWrite(value bool) Option {
	return func(opts *options) {
		opts.cfg.SyncWrite = value
	}
}

// WithMaxBatchingSize set MaxBatchingSize
func WithMaxBatchingSize(value int) Option {
	return func(opts *options) {
		opts.cfg.MaxBatchingSize = value
	}
}

// WithMaxDBRecords set MaxDBRecords
func WithMaxDBRecords(value uint64) Option {
	return func(opts *options) {
		opts.cfg.MaxDBRecords = value
	}
}

// WithApplyWorkerCount set ApplyWorkerCount
func WithApplyWorkerCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.ApplyWorkerCount = value
	}
}

// WithSentRaftWorkerCount set SentRaftWorkerCount
func WithSentRaftWorkerCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.SentRaftWorkerCount = value
	}
}

// WithSentSnapWorkerCount set SentSnapWorkerCount
func WithSentSnapWorkerCount(value uint64) Option {
	return func(opts *options) {
		opts.cfg.SentSnapWorkerCount = value
	}
}

// WithLimitSnapChunkRate set LimitSnapChunkRate
func WithLimitSnapChunkRate(value int) Option {
	return func(opts *options) {
		opts.cfg.LimitSnapChunkRate = value
	}
}

// WithLimitSnapChunkBytes set LimitSnapChunkBytes
func WithLimitSnapChunkBytes(value uint64) Option {
	return func(opts *options) {
		opts.cfg.LimitSnapChunkBytes = value
	}
}

// WithDBHBInterval set DBHBInterval
func WithDBHBInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.DBHBInterval = value
	}
}

// WithStoreHBInterval set StoreHBInterval
func WithStoreHBInterval(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.StoreHBInterval = value
	}
}

// WithMaxPeerDownTime set MaxPeerDownTime
func WithMaxPeerDownTime(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.MaxPeerDownTime = value
	}
}

// WithDim set Dim
func WithDim(value int) Option {
	return func(opts *options) {
		opts.cfg.Dim = value
	}
}

// WithFlatThr set FlatThr
func WithFlatThr(value int) Option {
	return func(opts *options) {
		opts.cfg.FlatThr = value
	}
}

// WithDistThr set DistThr
func WithDistThr(value float32) Option {
	return func(opts *options) {
		opts.cfg.DistThr = value
	}
}

// WithLimitRebuildIndex set LimitRebuildIndex
func WithLimitRebuildIndex(value int) Option {
	return func(opts *options) {
		opts.cfg.LimitRebuildIndex = value
	}
}

// WithRebuildIndexDuration set RebuildIndexDuration
func WithRebuildIndexDuration(value time.Duration) Option {
	return func(opts *options) {
		opts.cfg.RebuildIndexDuration = value
	}
}

// WithMQ set mq
func WithMQ(topic, groupPrefix string, mqAddrs []string) Option {
	return func(opts *options) {
		opts.cfg.Topic = topic
		opts.cfg.GroupPrefix = groupPrefix
		opts.cfg.MQAddrs = mqAddrs
	}
}
