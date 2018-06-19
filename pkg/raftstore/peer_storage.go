package raftstore

import (
	"errors"
	"fmt"
	"sync"

	etcdraft "github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/storage"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/vectordb"
)

const (
	// When we create a region peer, we should initialize its log term/index > 0,
	// so that we can force the follower peer to sync the snapshot first.
	raftInitLogTerm  = 5
	raftInitLogIndex = 5

	maxSnapTryCnt = 10
)

type peerStorage struct {
	store *Store
	db    meta.VectorDB
	vdb   vectordb.DB

	lastTerm         uint64
	appliedIndexTerm uint64
	lastReadyIndex   uint64
	lastCompactIndex uint64
	raftlocalState   raftpb.RaftLocalState
	raftApplyState   raftpb.RaftApplyState

	snapTriedCnt int
	genSnapJob   *task.Job
	applySnapJob *task.Job
	snapLock     sync.Mutex

	vectorRecords uint64
	inAsking      bool
}

func newPeerStorage(store *Store, db meta.VectorDB) (*peerStorage, error) {
	ps := new(peerStorage)
	ps.store = store
	ps.db = db
	ps.appliedIndexTerm = raftInitLogTerm

	err := ps.initRaftLocalState()
	if err != nil {
		return nil, err
	}
	log.Infof("raftstore[db-%d]: init raft local state: %v",
		db.ID,
		ps.raftlocalState)

	err = ps.initRaftApplyState()
	if err != nil {
		return nil, err
	}
	log.Infof("raftstore[db-%d]: init raft apply state: %v",
		db.ID,
		ps.raftApplyState)

	err = ps.initLastTerm()
	if err != nil {
		return nil, err
	}
	log.Infof("raftstore[db-%d]: init last term: %d",
		db.ID,
		ps.lastTerm)

	err = ps.initVectorDB()
	if err != nil {
		return nil, err
	}
	log.Infof("raftstore[db-%d]: init verctorDB",
		db.ID)

	ps.lastReadyIndex = ps.appliedIndex()
	return ps, nil
}

func (ps *peerStorage) initRaftLocalState() error {
	v, err := ps.store.metaStore.Get(getRaftLocalStateKey(ps.db.ID))
	if err != nil {
		return err
	}

	if len(v) > 0 {
		s := &raftpb.RaftLocalState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.raftlocalState = *s
		return nil
	}

	s := &raftpb.RaftLocalState{}
	if len(ps.db.Peers) > 0 {
		s.LastIndex = raftInitLogIndex
	}

	ps.raftlocalState = *s
	return nil
}

func (ps *peerStorage) initRaftApplyState() error {
	v, err := ps.store.metaStore.Get(getRaftApplyStateKey(ps.db.ID))
	if err != nil {
		return err
	}

	if len(v) > 0 && len(ps.db.Peers) > 0 {
		s := &raftpb.RaftApplyState{}
		err = s.Unmarshal(v)
		if err != nil {
			return err
		}

		ps.raftApplyState = *s
		return nil
	}

	if len(ps.db.Peers) > 0 {
		ps.raftApplyState.AppliedIndex = raftInitLogIndex
		ps.raftApplyState.TruncatedState.Index = raftInitLogIndex
		ps.raftApplyState.TruncatedState.Term = raftInitLogTerm
	}

	return nil
}

func (ps *peerStorage) initLastTerm() error {
	lastIndex := ps.raftlocalState.LastIndex

	if lastIndex == 0 {
		ps.lastTerm = lastIndex
		return nil
	} else if lastIndex == raftInitLogIndex {
		ps.lastTerm = raftInitLogTerm
		return nil
	} else if lastIndex == ps.raftApplyState.TruncatedState.Index {
		ps.lastTerm = ps.raftApplyState.TruncatedState.Term
		return nil
	} else if lastIndex < raftInitLogIndex {
		log.Fatalf("raftstore[db-%d]: error raft last index %d",
			ps.db.ID,
			lastIndex)
		return nil
	}

	v, err := ps.store.metaStore.Get(getRaftLogKey(ps.db.ID, lastIndex))
	if err != nil {
		return err
	}

	if nil == v {
		return fmt.Errorf("raftstore[db-%d]: log entry at index %d doesn't exist, may lose data",
			ps.db.ID,
			lastIndex)
	}

	s := &etcdraftpb.Entry{}
	err = s.Unmarshal(v)
	if err != nil {
		return err
	}

	ps.lastTerm = s.Term
	return nil
}

func (ps *peerStorage) initVectorDB() error {
	vdb, err := vectordb.NewDB(ps.store.cfg.getDBDir(ps.db.ID),
		ps.store.cfg.Dim,
		ps.store.cfg.FlatThr,
		ps.store.cfg.DistThr)
	if err != nil {
		return err
	}

	ps.vdb = vdb
	return err
}

func (ps *peerStorage) loadDBRaftApplyState() (*raftpb.RaftApplyState, error) {
	key := getRaftApplyStateKey(ps.db.ID)
	v, err := ps.store.metaStore.Get(key)
	if err != nil {
		log.Errorf("raftstore[db-%d]: load raft apply state failed, errors:\n %+v",
			ps.db.ID,
			err)
		return nil, err
	}

	if len(v) == 0 {
		return nil, errors.New("db apply state not found")
	}

	applyState := &raftpb.RaftApplyState{}
	err = applyState.Unmarshal(v)
	return applyState, err
}

func (ps *peerStorage) loadDBRaftLocalState(job *task.Job) (*raftpb.DBLocalState, error) {
	if nil != job &&
		job.IsCancelling() {
		return nil, task.ErrJobCancelled
	}

	return loadDBRaftLocalState(ps.db.ID, ps.store.metaStore, false)
}

func (ps *peerStorage) updatePeerState(db meta.VectorDB, state raftpb.PeerState, wb storage.WriteBatch) error {
	localState := &raftpb.DBLocalState{}
	localState.State = state
	localState.DB = db

	data, _ := localState.Marshal()
	if wb != nil {
		return wb.Set(getDBStateKey(db.ID), data)
	}

	return ps.store.metaStore.Set(getDBStateKey(db.ID), data, ps.store.cfg.SyncWrite)
}

func (ps *peerStorage) writeInitialState(id uint64, wb storage.WriteBatch) error {
	raftState := new(raftpb.RaftLocalState)
	raftState.LastIndex = raftInitLogIndex
	raftState.HardState.Term = raftInitLogTerm
	raftState.HardState.Commit = raftInitLogIndex

	applyState := new(raftpb.RaftApplyState)
	applyState.AppliedIndex = raftInitLogIndex
	applyState.TruncatedState.Index = raftInitLogIndex
	applyState.TruncatedState.Term = raftInitLogTerm

	err := wb.Set(getRaftLocalStateKey(id), pbutil.MustMarshal(raftState))
	if err != nil {
		return err
	}

	return wb.Set(getRaftApplyStateKey(id), pbutil.MustMarshal(applyState))
}

func loadDBRaftLocalState(id uint64, store storage.Storage, allowNotFound bool) (*raftpb.DBLocalState, error) {
	key := getDBStateKey(id)
	v, err := store.Get(key)
	if err != nil {
		log.Errorf("raftstore[db-%d]: load raft state failed, errors:\n %+v",
			id,
			err)
		return nil, err
	} else if len(v) == 0 {
		if allowNotFound {
			return nil, nil
		}

		return nil, errors.New("db state not found")
	}

	stat := &raftpb.DBLocalState{}
	err = stat.Unmarshal(v)
	return stat, err
}

func (ps *peerStorage) loadLogEntry(index uint64) (*etcdraftpb.Entry, error) {
	key := getRaftLogKey(ps.db.ID, index)
	v, err := ps.store.metaStore.Get(key)
	if err != nil {
		log.Errorf("raftstore[db-%d]: load entry %d failure, errors:\n %+v",
			ps.db.ID,
			index,
			err)
		return nil, err
	} else if len(v) == 0 {
		log.Errorf("raftstore[db-%d]: entry %d not found",
			ps.db.ID,
			index)
		return nil, fmt.Errorf("log entry %d not found", index)
	}

	return ps.unmarshalLogEntry(v, index)
}

/// Delete all data belong to the db.
/// If return Err, data may get partial deleted.
func (ps *peerStorage) clearData() error {
	return ps.vdb.Clean()
}

func (ps *peerStorage) isInitialized() bool {
	return len(ps.db.Peers) != 0
}

func (ps *peerStorage) committedIndex() uint64 {
	return ps.raftlocalState.HardState.Commit
}

func (ps *peerStorage) appliedIndex() uint64 {
	return ps.raftApplyState.AppliedIndex
}

func (ps *peerStorage) truncatedIndex() uint64 {
	return ps.raftApplyState.TruncatedState.Index
}

func (ps *peerStorage) truncatedTerm() uint64 {
	return ps.raftApplyState.TruncatedState.Term
}

func (ps *peerStorage) isApplyComplete() bool {
	return ps.committedIndex() == ps.appliedIndex()
}

// ====================== about jobs
func (ps *peerStorage) isApplyingSnap() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsNotComplete()
}

func (ps *peerStorage) isApplySnapComplete() bool {
	return ps.applySnapJob != nil && ps.applySnapJob.IsComplete()
}

func (ps *peerStorage) isGenSnapRunning() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsNotComplete()
}

func (ps *peerStorage) isGenSnapComplete() bool {
	return ps.genSnapJob != nil && ps.genSnapJob.IsComplete()
}

func (ps *peerStorage) setGenSnapJob(job *task.Job) {
	ps.genSnapJob = job
}

func (ps *peerStorage) setApplySnapJob(job *task.Job) {
	ps.applySnapJob = job
}

func (ps *peerStorage) resetGenSnapJob() {
	ps.genSnapJob = nil
	ps.snapTriedCnt = 0
}

func (ps *peerStorage) resetApplySnapJob() {
	ps.snapLock.Lock()
	ps.applySnapJob = nil
	ps.snapLock.Unlock()
}

func (ps *peerStorage) cancelApplySnapJob() bool {
	ps.snapLock.Lock()
	if ps.applySnapJob == nil {
		ps.snapLock.Unlock()
		return true
	}

	ps.applySnapJob.Cancel()

	if ps.applySnapJob.IsCancelled() {
		ps.snapLock.Unlock()
		return true
	}

	succ := ps.isApplySnapComplete()
	ps.snapLock.Unlock()
	return succ
}

// ======================raft storage interface method
func (ps *peerStorage) InitialState() (etcdraftpb.HardState, etcdraftpb.ConfState, error) {
	hardState := ps.raftlocalState.HardState
	confState := etcdraftpb.ConfState{}

	if etcdraft.IsEmptyHardState(hardState) {
		if ps.isInitialized() {
			log.Fatalf("raftstore[db-%d]: db is initialized but local state has empty hard state: %v",
				ps.db.ID,
				hardState)
		}

		return hardState, confState, nil
	}

	for _, p := range ps.db.Peers {
		confState.Nodes = append(confState.Nodes, p.ID)
	}

	return hardState, confState, nil
}

func (ps *peerStorage) Entries(low, high, maxSize uint64) ([]etcdraftpb.Entry, error) {
	err := ps.checkRange(low, high)
	if err != nil {
		return nil, err
	}

	var ents []etcdraftpb.Entry
	if low == high {
		return ents, nil
	}

	var totalSize uint64
	nextIndex := low
	exceededMaxSize := false

	startKey := getRaftLogKey(ps.db.ID, low)

	if low+1 == high {
		// If election happens in inactive cells, they will just try
		// to fetch one empty log.
		v, err := ps.store.metaStore.Get(startKey)
		if err != nil {
			return nil, err
		}

		if len(v) == 0 {
			return nil, etcdraft.ErrUnavailable
		}

		e, err := ps.unmarshalLogEntry(v, low)
		if err != nil {
			return nil, err
		}

		ents = append(ents, *e)
		return ents, nil
	}

	endKey := getRaftLogKey(ps.db.ID, high)
	err = ps.store.metaStore.Scan(startKey, endKey, func(key, value []byte) (bool, error) {
		e := &etcdraftpb.Entry{}
		pbutil.MustUnmarshal(e, value)

		// May meet gap or has been compacted.
		if e.Index != nextIndex {
			return false, nil
		}

		nextIndex++
		totalSize += uint64(len(value))

		exceededMaxSize = totalSize > maxSize
		if !exceededMaxSize || len(ents) == 0 {
			ents = append(ents, *e)
		}

		return !exceededMaxSize, nil
	})

	if err != nil {
		return nil, err
	}

	// If we get the correct number of entries the total size exceeds max_size, returns.
	if len(ents) == int(high-low) || exceededMaxSize {
		return ents, nil
	}

	return nil, etcdraft.ErrUnavailable
}

func (ps *peerStorage) Term(idx uint64) (uint64, error) {
	if idx == ps.truncatedIndex() {
		return ps.truncatedTerm(), nil
	}

	err := ps.checkRange(idx, idx+1)
	if err != nil {
		return 0, err
	}

	lastIdx, err := ps.LastIndex()
	if err != nil {
		return 0, err
	}

	if ps.truncatedTerm() == ps.lastTerm || idx == lastIdx {
		return ps.lastTerm, nil
	}

	key := getRaftLogKey(ps.db.ID, idx)
	v, err := ps.store.metaStore.Get(key)
	if err != nil {
		return 0, err
	}

	if v == nil {
		return 0, etcdraft.ErrUnavailable
	}

	e, err := ps.unmarshalLogEntry(v, idx)
	if err != nil {
		return 0, err
	}

	t := e.Term
	return t, nil
}

func (ps *peerStorage) LastIndex() (uint64, error) {
	return ps.raftlocalState.LastIndex, nil
}

func (ps *peerStorage) FirstIndex() (uint64, error) {
	return ps.truncatedIndex() + 1, nil
}

func (ps *peerStorage) Snapshot() (etcdraftpb.Snapshot, error) {
	if ps.isGenSnapRunning() {
		return etcdraftpb.Snapshot{}, etcdraft.ErrSnapshotTemporarilyUnavailable
	}

	if ps.isGenSnapComplete() {
		result := ps.genSnapJob.GetResult()
		// snapshot failure, we will continue try do snapshot
		if nil == result {
			log.Warnf("raftstore[db-%d]: snapshot generating failed, tried %d times",
				ps.db.ID,
				ps.snapTriedCnt)
			ps.snapTriedCnt++
		} else {
			snap := result.(etcdraftpb.Snapshot)
			if ps.validateSnap(&snap) {
				ps.resetGenSnapJob()
				return snap, nil
			}
		}
	}

	if ps.snapTriedCnt >= maxSnapTryCnt {
		cnt := ps.snapTriedCnt
		ps.resetGenSnapJob()
		return etcdraftpb.Snapshot{}, fmt.Errorf("db %d failed to get snapshot after %d times",
			ps.db.ID,
			cnt)
	}

	log.Infof("raftstore[db-%d]: start snapshot, epoch=<%+v>",
		ps.db.ID,
		ps.db.Epoch)
	ps.snapTriedCnt++

	err := ps.store.addSnapJob(ps.doGenSnapJob, ps.setGenSnapJob)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: add generate job failed, errors:\n %+v",
			ps.db.ID,
			err)
	}

	return etcdraftpb.Snapshot{}, etcdraft.ErrSnapshotTemporarilyUnavailable
}

func (ps *peerStorage) checkRange(low, high uint64) error {
	if low > high {
		return fmt.Errorf("raftstore[db-%d]: low %d is greater that high %d",
			ps.db.ID,
			low,
			high)
	} else if low <= ps.truncatedIndex() {
		return etcdraft.ErrCompacted
	} else {
		i, err := ps.LastIndex()
		if err != nil {
			return err
		}

		if high > i+1 {
			return fmt.Errorf("raftstore[db-%d]: entries' high %d is out of bound lastindex %d",
				ps.db.ID,
				high,
				i)
		}
	}

	return nil
}

func (ps *peerStorage) unmarshalLogEntry(v []byte, expectIndex uint64) (*etcdraftpb.Entry, error) {
	e := &etcdraftpb.Entry{}
	if err := e.Unmarshal(v); err != nil {
		log.Errorf("raftstore[db-%d]: unmarshal entry %d failure, data %+v errors:\n %+v",
			ps.db.ID,
			expectIndex,
			v,
			err)
		return nil, err
	}

	if e.Index != expectIndex {
		log.Fatalf("raftstore[db-%d]: raft log index %d not match %d",
			ps.db.ID,
			e.Index,
			expectIndex)
	}

	return e, nil
}

func (ps *peerStorage) validateSnap(snap *etcdraftpb.Snapshot) bool {
	idx := snap.Metadata.Index

	if idx < ps.truncatedIndex() {
		// stale snapshot, should generate again.
		log.Infof("raftstore[db-%d]: snapshot is stale, generate again, snapIndex %d, currIndex %d",
			ps.db.ID,
			idx,
			ps.truncatedIndex())
		return false
	}

	snapData := &raftpb.SnapshotMessage{}
	err := snapData.Unmarshal(snap.Data)
	if err != nil {
		log.Errorf("raftstore[db-%d]: decode snapshot fail, errors:\n %+v",
			ps.db.ID,
			err)
		return false
	}

	snapEpoch := snapData.Header.DB.Epoch
	lastEpoch := ps.db.Epoch

	if snapEpoch.ConfVersion < lastEpoch.ConfVersion {
		log.Infof("raftstore[db-%d]: snapshot epoch stale, generate again. snap is %d, curr is %d",
			ps.db.ID,
			snapEpoch.ConfVersion,
			lastEpoch.ConfVersion)
		return false
	}

	return true
}
