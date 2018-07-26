package prophet

import (
	"sync"
	"time"

	"github.com/fagongzi/goetty"
)

// RoleChangeHandler prophet role change handler
type RoleChangeHandler interface {
	BecomeLeader()
	BecomeFollower()
}

// Adapter prophet adapter
type Adapter interface {
	// NewResource return a new resource
	NewResource() Resource
	// NewContainer return a new container
	NewContainer() Container
	// FetchResourceHB fetch resource HB
	FetchAllResourceHB() []*ResourceHeartbeatReq
	// FetchResourceHB fetch resource HB
	FetchResourceHB(id uint64) *ResourceHeartbeatReq
	// FetchContainerHB fetch container HB
	FetchContainerHB() *ContainerHeartbeatReq
	// ResourceHBInterval fetch resource HB interface
	ResourceHBInterval() time.Duration
	// ContainerHBInterval fetch container HB interface
	ContainerHBInterval() time.Duration
	// HBHandler HB hander
	HBHandler() HeartbeatHandler
}

// Prophet is the distributed scheduler and coordinator
type Prophet struct {
	sync.Mutex
	adapter     Adapter
	cfg         *Cfg
	store       Store
	rt          *Runtime
	coordinator *Coordinator
	node        *Node
	leader      *Node
	leaderFlag  int64
	signature   string
	tcpL        *goetty.Server
	runner      *Runner
	completeC   chan struct{}
	rpc         *simpleRPC
	bizCodec    *codec

	wn *watcherNotifier
}

// NewProphet returns a prophet instance
func NewProphet(name string, addr string, adapter Adapter, opts ...Option) *Prophet {
	value := &options{cfg: &Cfg{}}
	for _, opt := range opts {
		opt(value)
	}
	value.adjust()

	p := new(Prophet)
	p.cfg = value.cfg
	p.adapter = adapter
	p.bizCodec = &codec{adapter: adapter}
	p.leaderFlag = 0
	p.node = &Node{
		Name: name,
		Addr: addr,
	}
	p.signature = p.node.marshal()
	p.store = newEtcdStore(value.client, p.cfg.Namespace, adapter, p.signature)
	p.runner = NewRunner()
	p.coordinator = newCoordinator(value.cfg, p.runner, p.rt)
	p.tcpL = goetty.NewServer(addr,
		goetty.WithServerDecoder(goetty.NewIntLengthFieldBasedDecoder(p.bizCodec)),
		goetty.WithServerEncoder(goetty.NewIntLengthFieldBasedEncoder(p.bizCodec)))
	p.completeC = make(chan struct{})
	p.rpc = newSimpleRPC(p)

	return p
}

// Start start the prophet
func (p *Prophet) Start() {
	p.startListen()
	p.startLeaderLoop()
	p.startResourceHeartbeatLoop()
	p.startContainerHeartbeatLoop()
}

// GetStore returns the store
func (p *Prophet) GetStore() Store {
	return p.store
}

// GetRPC returns the rpc interface
func (p *Prophet) GetRPC() RPC {
	return p.rpc
}
