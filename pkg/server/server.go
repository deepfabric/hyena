package server

import (
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/hyena/pkg/raftstore"
)

// Server serve the search,insert,and update requests
type Server struct {
	opts           *options
	addr, raftAddr string

	tcpL     *goetty.Server
	sessions *sync.Map

	// raftstore
	store *raftstore.Store

	// tasks
	runner   *task.Runner
	stopOnce sync.Once
	stopWG   sync.WaitGroup
	stopC    chan struct{}
}

// NewServer returns server for serve vectordb requests
func NewServer(addr, raftAddr string, opts ...Option) *Server {
	sopts := &options{}
	for _, opt := range opts {
		opt(sopts)
	}
	sopts.adjust()

	meta := meta.Store{
		Address:       raftAddr,
		ClientAddress: addr,
		State:         meta.UP,
	}

	svr := &Server{
		addr:     addr,
		raftAddr: raftAddr,
		runner:   task.NewRunner(),
		stopC:    make(chan struct{}),
		store:    raftstore.NewStore(meta, sopts.raftOptions...),
		sessions: &sync.Map{},
		tcpL: goetty.NewServer(addr,
			goetty.WithServerDecoder(codec.GetDecoder()),
			goetty.WithServerEncoder(codec.GetEncoder())),
	}

	return svr
}

// Start start the server
func (s *Server) Start() {
	log.Infof("begin to start hyena")
	go s.listenToStop()
	s.startTCP()
	s.store.Start()
}

// Stop stop the server
func (s *Server) Stop() {
	s.stopWG.Add(1)
	s.stopC <- struct{}{}
	s.stopWG.Wait()
}

func (s *Server) listenToStop() {
	<-s.stopC
	s.doStop()
}

func (s *Server) doStop() {
	s.stopOnce.Do(func() {
		defer s.stopWG.Done()

		s.runner.Stop()
		s.store.Stop()
	})
}

func (s *Server) startTCP() {
	go func() {
		err := s.tcpL.Start(s.doConnection)
		if err != nil {
			log.Fatalf("start listen at %s failed, errors:%+v",
				s.addr,
				err)
		}
	}()
	<-s.tcpL.Started()
	log.Infof("hyena listen at %s", s.addr)
}

func (s *Server) doConnection(conn goetty.IOSession) error {
	addr := conn.RemoteAddr()
	log.Debugf("net: %s connected", addr)

	client := newSession(conn)
	s.sessions.Store(client.id, client)
	defer func() {
		s.sessions.Delete(client.id)
		client.close()
	}()

	// The client usually is a proxy, the proxy can send insert,update,search or batch request,
	// batch request only contains one kind request, not support mixing batch request.
	for {
		_, err := conn.ReadTimeout(s.opts.timeoutRead)
		if err != nil {
			return err
		}

	}
}
