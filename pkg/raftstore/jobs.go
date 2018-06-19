package raftstore

import (
	"fmt"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
)

func (pr *PeerReplicate) startRegistrationJob() {
	delegate := &applyDelegate{
		store:            pr.store,
		ps:               pr.ps,
		peerID:           pr.peer.ID,
		db:               pr.ps.db,
		term:             pr.getCurrentTerm(),
		applyState:       pr.ps.raftApplyState,
		appliedIndexTerm: pr.ps.appliedIndexTerm,
	}

	err := pr.store.addApplyJob(pr.id, "doRegistrationJob", func() error {
		return pr.doRegistrationJob(delegate)
	}, nil)

	if err != nil {
		log.Fatalf("raftstore[db-%d]: add registration job failed, errors:\n %+v",
			pr.id,
			err)
	}
}

func (pr *PeerReplicate) startProposeJob(c *cmd, isConfChange bool) error {
	err := pr.store.addApplyJob(pr.id, "doProposeJob", func() error {
		return pr.doProposeJob(c, isConfChange)
	}, nil)

	return err
}

func (pr *PeerReplicate) startApplySnapJob() {
	pr.ps.snapLock.Lock()
	err := pr.store.addApplyJob(pr.id, "doApplyingSnapshotJob", pr.doApplySnapJob, pr.ps.setApplySnapJob)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: add apply snapshot task fail, errors:\n %+v",
			pr.id,
			err)
	}
	pr.ps.snapLock.Unlock()
}

func (pr *PeerReplicate) startApplyCommittedEntriesJob(commitedEntries []etcdraftpb.Entry) error {
	term := pr.getCurrentTerm()
	err := pr.store.addApplyJob(pr.id, "doApplyCommittedEntries", func() error {
		return pr.doApplyCommittedEntries(term, commitedEntries)
	}, nil)
	return err
}

func (s *Store) startDestroyJob(id uint64, peer meta.Peer) error {
	err := s.addApplyJob(id, "doDestroy", func() error {
		return s.doDestroy(id, peer)
	}, nil)

	return err
}

func (pr *PeerReplicate) startCompactRaftLogJob(id, startIndex, endIndex uint64) error {
	err := pr.store.addCompactRaftLogJob(func() error {
		return pr.doCompactRaftLog(id, startIndex, endIndex)
	})

	return err
}

func (pr *PeerReplicate) startAskSplitJob(db meta.VectorDB) error {
	err := pr.store.addSplitJob(func() error {
		return pr.doAskSplit(db)
	})

	return err
}

func (pr *PeerReplicate) doRegistrationJob(delegate *applyDelegate) error {
	if value, ok := pr.store.delegates.Load(delegate.db.ID); ok {
		old := value.(*applyDelegate)
		if old.peerID != delegate.peerID {
			log.Fatalf("raftstore[db-%d]: delegate peer id not match, old %d curr %d",
				pr.id,
				old.peerID,
				delegate.peerID)
		}

		old.term = delegate.term
		old.clearAllCommandsAsStale()
	}

	pr.store.delegates.Store(delegate.db.ID, delegate)
	return nil
}

func (pr *PeerReplicate) doProposeJob(c *cmd, isConfChange bool) error {
	value, ok := pr.store.delegates.Load(pr.id)
	if !ok {
		c.respError(errorDBNotFound(pr.id))
		return nil
	}

	delegate := value.(*applyDelegate)
	if delegate.db.ID != pr.id {
		log.Fatal("bug: delegate id not match")
	}

	if isConfChange {
		changeC := delegate.getPendingChangePeerCMD()
		if nil != changeC && changeC.req != nil {
			delegate.notifyStaleCMD(changeC)
		}
		delegate.setPendingChangePeerCMD(c)
	} else {
		delegate.appendPendingCmd(c)
	}

	return nil
}

func (ps *peerStorage) doGenSnapJob() error {
	if ps.genSnapJob == nil {
		log.Fatalf("raftstore[db-%d]: generating snapshot job is nil", ps.db.ID)
	}

	applyState, err := ps.loadDBRaftApplyState()
	if err != nil {
		log.Fatalf("raftstore[db-%d]: load snapshot failure, errors:\n %+v",
			ps.db.ID,
			err)
	} else if nil == applyState {
		log.Fatalf("raftstore[db-%d]: could not load snapshot", ps.db.ID)
	}

	var term uint64
	if applyState.AppliedIndex == applyState.TruncatedState.Index {
		term = applyState.TruncatedState.Term
	} else {
		entry, err := ps.loadLogEntry(applyState.AppliedIndex)
		if err != nil {
			return nil
		}

		term = entry.Term
	}

	state, err := ps.loadDBRaftLocalState(nil)
	if err != nil {
		return nil
	}
	if state.State != raftpb.Normal {
		log.Errorf("raftstore[db-%d]: snap seems stale, skip", ps.db.ID)
		return nil
	}

	msg := &raftpb.SnapshotMessage{}
	msg.Header.DB = state.DB
	msg.Header.Term = term
	msg.Header.Index = applyState.AppliedIndex

	snapshot := etcdraftpb.Snapshot{}
	snapshot.Metadata.Term = msg.Header.Term
	snapshot.Metadata.Index = msg.Header.Index

	confState := etcdraftpb.ConfState{}
	for _, peer := range ps.db.Peers {
		confState.Nodes = append(confState.Nodes, peer.ID)
	}
	snapshot.Metadata.ConfState = confState

	if ps.store.snapMgr.Register(msg, creating) {
		defer ps.store.snapMgr.Deregister(msg, creating)

		err = ps.store.snapMgr.Create(msg)
		if err != nil {
			log.Errorf("raftstore[db-%d]: create snapshot file failure, errors:\n %+v",
				ps.db.ID,
				err)
			return nil
		}
	}

	snapshot.Data = pbutil.MustMarshal(msg)
	ps.genSnapJob.SetResult(snapshot)

	log.Infof("raftstore[db-%d]: snapshot created, epoch is %d, term is %d index is %d",
		ps.db.ID,
		msg.Header.DB.Epoch,
		msg.Header.Term,
		msg.Header.Index)

	return nil
}

func (pr *PeerReplicate) doApplySnapJob() error {
	log.Infof("raftstore[db-%d]: begin apply snap data", pr.id)
	_, err := pr.ps.loadDBRaftLocalState(pr.ps.applySnapJob)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: apply snap load local state failed, errors:\n %+v",
			pr.id,
			err)
		return err
	}

	err = pr.ps.vdb.Clean()
	if err != nil {
		log.Fatalf("raftstore[db-%d]: apply snap delete db data failed, errors:\n %+v",
			pr.id,
			err)
		return err
	}

	err = pr.ps.applySnap(pr.ps.applySnapJob)
	if err != nil {
		log.Errorf("raftstore[db-%d]: apply snap snapshot failed, errors:\n %+v",
			pr.id,
			err)
		return err
	}

	err = pr.ps.updatePeerState(pr.ps.db, raftpb.Normal, nil)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: apply snap update peer state failed, errors:\n %+v",
			pr.id,
			err)
	}

	pr.stopRaftTick = false
	pr.store.removePendingSnap(pr.id)

	records, err := pr.ps.vdb.Records()
	if err != nil {
		log.Fatalf("raftstore[db-%d]: apply snap update peer state failed, errors:\n %+v",
			pr.id,
			err)
	}
	pr.ps.vectorRecords = records

	log.Infof("raftstore[db-%d]: apply snap complete", pr.id)
	return nil
}

func (ps *peerStorage) applySnap(job *task.Job) error {
	if nil != job &&
		job.IsCancelling() {
		return task.ErrJobCancelled
	}

	snap := &raftpb.SnapshotMessage{}
	snap.Header.DB = ps.db
	snap.Header.Term = ps.truncatedTerm()
	snap.Header.Index = ps.truncatedIndex()
	return ps.store.snapMgr.Apply(snap)
}

func (pr *PeerReplicate) doApplyCommittedEntries(term uint64, commitedEntries []etcdraftpb.Entry) error {
	value, ok := pr.store.delegates.Load(pr.id)
	if !ok {
		return fmt.Errorf("raftstore[db-%d]: missing delegate", pr.id)
	}

	delegate := value.(*applyDelegate)
	delegate.term = term
	delegate.applyCommittedEntries(commitedEntries)

	if delegate.isPendingRemove() {
		delegate.destroy()
		pr.store.delegates.Delete(delegate.db.ID)
	}

	return nil
}

func (s *Store) doDestroy(id uint64, peer meta.Peer) error {
	d, ok := s.delegates.Load(id)
	s.delegates.Delete(id)

	if ok {
		d.(*applyDelegate).destroy()
		s.destroyPeer(id, peer, false)
	} else {
		s.destroyPeer(id, peer, false)
	}

	return nil
}

func (pr *PeerReplicate) doCompactRaftLog(id, startIndex, endIndex uint64) error {
	firstIndex := startIndex

	if firstIndex == 0 {
		startKey := getRaftLogKey(id, 0)
		firstIndex = endIndex
		key, _, err := pr.store.metaStore.Seek(startKey)
		if err != nil {
			return err
		}

		if key != nil {
			firstIndex, err = getRaftLogIndex(key)
			if err != nil {
				return err
			}
		}
	}

	if firstIndex >= endIndex {
		log.Infof("raftstore[db-%d]: no need to gc raft log",
			id)
		return nil
	}

	wb := pr.store.metaStore.NewWriteBatch()
	for index := firstIndex; index < endIndex; index++ {
		key := getRaftLogKey(id, index)
		err := wb.Delete(key)
		if err != nil {
			return err
		}
	}

	err := pr.store.metaStore.Write(wb, false)
	if err != nil {
		log.Infof("raftstore[db-%d]: compact raft log complete, entriesCount=<%d>",
			id,
			(endIndex - startIndex))
	}

	return err
}

func (pr *PeerReplicate) doAskSplit(db meta.VectorDB) error {
	id, peerIDs, err := pr.store.pd.GetRPC().AskSplit(&ResourceAdapter{meta: db})
	if err != nil {
		log.Errorf("raftstore-split[db-%d]: ask split to pd failed, error:\n %+v",
			db.ID,
			err)
		return err
	}

	splitReq := new(raftpb.SplitRequest)
	splitReq.NewID = id
	splitReq.NewPeerIDs = peerIDs

	pr.onAdmin(&raftpb.AdminRequest{
		Type:  raftpb.Split,
		Split: splitReq,
	})
	return nil
}

func (s *Store) addProphetJob(task func() error, cb func(*task.Job)) error {
	return s.addNamedJobWithCB("", prophetWorker, task, cb)
}

func (s *Store) addCompactRaftLogJob(task func() error) error {
	return s.addNamedJob("", logCompactWorker, task)
}

func (s *Store) addSnapJob(task func() error, cb func(*task.Job)) error {
	return s.addNamedJobWithCB("", snapWorker, task, cb)
}

func (s *Store) addApplyJob(id uint64, desc string, task func() error, cb func(*task.Job)) error {
	index := (s.cfg.ApplyWorkerCount - 1) & id
	return s.addNamedJobWithCB(desc, fmt.Sprintf(applyWorker, index), task, cb)
}

func (s *Store) addSplitJob(task func() error) error {
	return s.addNamedJob("", splitWorker, task)
}

func (s *Store) addNamedJob(desc, worker string, task func() error) error {
	return s.runner.RunJobWithNamedWorker(desc, worker, task)
}

func (s *Store) addNamedJobWithCB(desc, worker string, task func() error, cb func(*task.Job)) error {
	return s.runner.RunJobWithNamedWorkerWithCB(desc, worker, task, cb)
}
