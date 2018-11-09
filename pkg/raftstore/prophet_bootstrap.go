package raftstore

import (
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/prophet"
)

func (s *Store) startProphet() {
	log.Infof("raftstore: start prophet")

	s.pdStartedC = make(chan struct{})
	adapter := &ProphetAdapter{store: s}
	s.cfg.ProphetOptions = append(s.cfg.ProphetOptions, prophet.WithRoleChangeHandler(s))
	s.pd = prophet.NewProphet(s.cfg.ProphetName, s.cfg.ProphetAddr, adapter, s.cfg.ProphetOptions...)

	s.pd.Start()
	<-s.pdStartedC
}

// BecomeLeader this node is become prophet leader
func (s *Store) BecomeLeader() {
	log.Infof("raftstore: BecomeLeader prophet")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		s.pdStartedC <- struct{}{}
	})
	log.Infof("raftstore: BecomeLeader prophet complete")
}

// BecomeFollower this node is become prophet follower
func (s *Store) BecomeFollower() {
	log.Infof("raftstore: BecomeFollower prophet")
	s.bootOnce.Do(func() {
		s.doBootstrapCluster()
		s.pdStartedC <- struct{}{}
	})
	log.Infof("raftstore: BecomeFollower prophet complete")
}

func (s *Store) doBootstrapCluster() {
	data, err := s.metaStore.Get(storeIdentKey)
	if err != nil {
		log.Fatalf("raftstore: load current store meta failed, errors:%+v", err)
	}

	if len(data) > 0 {
		s.meta.ID = goetty.Byte2UInt64(data)
		if s.meta.ID > 0 {
			log.Infof("raftstore: load from local, store is %d", s.meta.ID)
			return
		}
	}

	s.meta.ID = s.allocID()
	log.Infof("raftstore: init store with id: %d", s.meta.ID)

	count := 0
	err = s.metaStore.Scan(minKey, maxKey, func([]byte, []byte) (bool, error) {
		count++
		return false, nil
	})
	if err != nil {
		log.Fatalf("raftstore: bootstrap store failed, errors:\n %+v", err)
	}
	if count > 0 {
		log.Fatal("raftstore: store is not empty and has already had data")
	}

	err = s.metaStore.Set(storeIdentKey, goetty.Uint64ToBytes(s.meta.ID), true)
	if err != nil {
		log.Fatal("raftstore: save current store id failed, errors:%+v", err)
	}

	db := s.createFirstDB()
	ok, err := s.pd.GetStore().PutBootstrapped(&ContainerAdapter{meta: s.meta}, &ResourceAdapter{meta: db})
	if err != nil {
		derr := s.deleteDB(db.ID)
		if derr != nil {
			log.Errorf("raftstore: delete first db failed, errors:%+v", derr)
		}

		log.Fatal("raftstore: bootstrap cluster failed, errors:%+v", err)
	}
	if !ok {
		log.Info("raftstore: the cluster is already bootstrapped")

		derr := s.deleteDB(db.ID)
		if derr != nil {
			log.Fatal("raftstore: delete first db failed, errors:%+v", derr)
		}

		log.Info("raftstore: the first db is already removed from store")
	}

	s.pd.GetRPC().TiggerContainerHeartbeat()
}

func (s *Store) createFirstDB() meta.VectorDB {
	db := meta.VectorDB{
		ID:    s.allocID(),
		State: meta.RWU,
		Start: 0,
	}
	db.Peers = append(db.Peers, &meta.Peer{
		ID:      s.allocID(),
		StoreID: s.meta.ID,
	})

	err := s.saveDB(db)
	if err != nil {
		log.Fatalf("raftstore: bootstrap first db failed, errors:%+v", err)
	}

	log.Infof("raftstore: save first DB complete")
	return db
}

func (s *Store) allocID() uint64 {
	id, err := s.pd.GetRPC().AllocID()
	if err != nil {
		log.Fatalf("raftstore: alloc id failed, errors:%+v", err)
	}
	return id
}

func (s *Store) saveDB(db meta.VectorDB) error {
	wb := s.metaStore.NewWriteBatch()

	// save state
	err := wb.Set(getDBStateKey(db.ID), pbutil.MustMarshal(&raftpb.DBLocalState{DB: db}))
	if err != nil {
		return err
	}

	raftLocalState := new(raftpb.RaftLocalState)
	raftLocalState.LastIndex = raftInitLogIndex
	raftLocalState.HardState = etcdraftpb.HardState{
		Term:   raftInitLogTerm,
		Commit: raftInitLogIndex,
	}
	err = wb.Set(getRaftLocalStateKey(db.ID), pbutil.MustMarshal(raftLocalState))
	if err != nil {
		return err
	}

	raftApplyState := new(raftpb.RaftApplyState)
	raftApplyState.AppliedIndex = raftInitLogIndex
	raftApplyState.TruncatedState = raftpb.RaftTruncatedState{
		Term:  raftInitLogTerm,
		Index: raftInitLogIndex,
	}
	raftApplyState.CommittedOffset = mqInitCommittedOffset
	err = wb.Set(getRaftApplyStateKey(db.ID), pbutil.MustMarshal(raftApplyState))
	if err != nil {
		return err
	}

	log.Infof("raftstore: begin to write first db to local")
	return s.metaStore.Write(wb, true)
}

func (s *Store) deleteDB(id uint64) error {
	wb := s.metaStore.NewWriteBatch()

	// save state
	err := wb.Delete(getDBStateKey(id))
	if err != nil {
		return err
	}

	err = wb.Delete(getRaftLocalStateKey(id))
	if err != nil {
		return err
	}

	err = wb.Delete(getRaftApplyStateKey(id))
	if err != nil {
		return err
	}

	log.Infof("raftstore: delete first db from local")
	return s.metaStore.Write(wb, true)
}
