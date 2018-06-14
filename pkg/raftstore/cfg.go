package raftstore

import (
	"fmt"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/prophet"
)

// Cfg raftstore configuration
type Cfg struct {
	DataPath         string
	StoreOptionsPath string
	Lables           []meta.Label

	ProphetName    string
	ProphetAddr    string
	ProphetOptions []prophet.Option

	// about raft
	ElectionTick           int
	HeartbeatTick          int
	MaxSizePerMsg          uint64
	MaxInflightMsgs        int
	MinRaftLogCount        uint64
	MaxRaftLogCount        uint64
	MaxRaftLogBytes        uint64
	MaxRaftLogLag          uint64
	MaxRaftEntryBytes      uint64
	MaxPeerDownTime        time.Duration
	RaftTickDuration       time.Duration
	RaftLogCompactDuration time.Duration
	SyncWrite              bool

	// about batching
	MaxBatchingSize int

	// about db
	MaxDBRecords uint64

	// about worker
	ApplyWorkerCount    uint64
	SentRaftWorkerCount uint64
	SentSnapWorkerCount uint64

	// about snapshot
	LimitSnapChunkRate  int
	LimitSnapChunkBytes uint64

	// about prophet
	DBHBInterval    time.Duration
	StoreHBInterval time.Duration

	// about vectordb
	Dim     int
	FlatThr int
	DistThr float32
}

// Adjust adjust
func (c *Cfg) Adjust() {
	if c.RaftTickDuration == 0 {
		c.RaftTickDuration = time.Second
	}

	if c.ElectionTick == 0 {
		c.ElectionTick = 10
	}

	if c.HeartbeatTick == 0 {
		c.HeartbeatTick = 2
	}

	if c.MaxSizePerMsg == 0 {
		c.MaxSizePerMsg = 1024 * 1024
	}

	if c.MaxInflightMsgs == 0 {
		c.MaxInflightMsgs = 256
	}

	if c.MinRaftLogCount == 0 {
		c.MinRaftLogCount = 64
	}

	if c.MaxRaftLogCount == 0 {
		c.MaxRaftLogCount = 1024
	}

	if c.MaxRaftLogBytes == 0 {
		c.MaxRaftLogBytes = 32 * 1024 * 1024
	}

	if c.MaxRaftEntryBytes == 0 {
		c.MaxRaftEntryBytes = 8 * 1024 * 1024
	}

	if c.MaxPeerDownTime == 0 {
		c.MaxPeerDownTime = time.Second * 5 * 60
	}

	if c.RaftLogCompactDuration == 0 {
		c.RaftLogCompactDuration = time.Second * 10
	}

	if c.MaxBatchingSize == 0 {
		c.MaxBatchingSize = 1024
	}

	if c.MaxDBRecords == 0 {
		c.MaxDBRecords = 1000000
	}

	if c.ApplyWorkerCount == 0 {
		c.ApplyWorkerCount = 1
	}

	if c.SentRaftWorkerCount == 0 {
		c.SentRaftWorkerCount = 8
	}

	if c.SentSnapWorkerCount == 0 {
		c.SentSnapWorkerCount = 4
	}

	if c.LimitSnapChunkRate == 0 {
		c.LimitSnapChunkRate = 16
	}

	if c.LimitSnapChunkBytes == 0 {
		c.LimitSnapChunkBytes = 1024
	}

	if c.DBHBInterval == 0 {
		c.DBHBInterval = time.Second * 10
	}

	if c.StoreHBInterval == 0 {
		c.StoreHBInterval = time.Second * 30
	}
}

func (c *Cfg) getSnapDir() string {
	return fmt.Sprintf("%s/%s", c.DataPath, getSnapDirName())
}

func (c *Cfg) getDBDir(id uint64) string {
	return fmt.Sprintf("%s/%s/%d", c.DataPath, getDBDirName(), id)
}

func (c *Cfg) getRaftConfig(id, appliedIndex uint64, store raft.Storage) *raft.Config {
	return &raft.Config{
		ID:              id,
		Applied:         appliedIndex,
		ElectionTick:    c.ElectionTick,
		HeartbeatTick:   c.HeartbeatTick,
		MaxSizePerMsg:   c.MaxSizePerMsg,
		MaxInflightMsgs: c.MaxInflightMsgs,
		Storage:         store,
		CheckQuorum:     true,
		PreVote:         false,
	}
}

func getSnapDirName() string {
	return "snap"
}

func getDBDirName() string {
	return "dbs"
}
