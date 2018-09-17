package raftstore

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/storage"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/task"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/prophet"
)

const (
	applyWorker      = "apply-worker-%d"
	genSnapWorker    = "gen-snap-worker"
	splitWorker      = "split-worker"
	prophetWorker    = "prophet-worker"
	logCompactWorker = "log-compact-worker"
)

// Store manager local raft groups
type Store struct {
	cfg *Cfg

	meta       meta.Store
	metaStore  storage.Storage
	runner     *task.Runner
	pd         *prophet.Prophet
	pdStartedC chan struct{}
	snapMgr    SnapshotManager
	trans      *transport

	peers           *sync.Map
	replicates      *sync.Map
	pendingSnaps    *sync.Map
	delegates       *sync.Map
	droppedVoteMsgs *sync.Map

	// metrics
	startAt            uint32
	reveivingSnapCount uint64

	firstDBLoaded uint64
}

// NewStore returns store
func NewStore(meta meta.Store, opts ...Option) *Store {
	sopts := &options{
		cfg: &Cfg{},
	}
	for _, op := range opts {
		op(sopts)
	}
	sopts.adjust()

	return NewStoreWithCfg(meta, sopts.cfg)
}

// NewStoreWithCfg returns store with cfg
func NewStoreWithCfg(meta meta.Store, cfg *Cfg) *Store {
	meta.Lables = cfg.Lables

	s := new(Store)
	s.cfg = cfg
	s.startAt = uint32(time.Now().Unix())
	s.meta = meta
	s.metaStore = storage.NewStorage(storage.WithNemoDataPath(cfg.DataPath))
	s.snapMgr = newDefaultSnapshotManager(s)
	s.trans = newTransport(s, s.notify)

	s.peers = &sync.Map{}
	s.replicates = &sync.Map{}
	s.pendingSnaps = &sync.Map{}
	s.delegates = &sync.Map{}
	s.droppedVoteMsgs = &sync.Map{}

	s.runner = task.NewRunner()
	for i := 0; i < int(s.cfg.ApplyWorkerCount); i++ {
		s.runner.AddNamedWorker(fmt.Sprintf(applyWorker, i))
	}
	s.runner.AddNamedWorker(prophetWorker)
	s.runner.AddNamedWorker(splitWorker)
	s.runner.AddNamedWorker(logCompactWorker)
	s.runner.AddNamedWorker(genSnapWorker)

	return s
}

// Start returns the error when start store
func (s *Store) Start() {
	s.startProphet()

	log.Infof("raftstore: begin to start store %d", s.meta.ID)

	go s.startTransfer()
	<-s.trans.server.Started()
	log.Infof("raftstore: raft listen at %s", s.meta.Address)

	s.startDBs()
	log.Infof("raftstore: dbs started")

	s.startRaftCompact()
	log.Infof("raftstore: ready to handle raft log task")
}

// Stop returns the error when stop store
func (s *Store) Stop() error {
	s.replicates.Range(func(key, value interface{}) bool {
		value.(*PeerReplicate).stopEventLoop()
		return true
	})

	s.trans.stop()
	err := s.runner.Stop()
	return err
}

func (s *Store) startTransfer() {
	err := s.trans.start()
	if err != nil {
		log.Fatalf("raftstore: start transfer failed, errors:\n %+v", err)
	}
}

func (s *Store) startDBs() {
	totalCount := 0
	tomebstoneCount := 0
	applyingCount := 0

	var states []*raftpb.DBLocalState
	wb := s.metaStore.NewWriteBatch()
	err := s.metaStore.Scan(dbMetaMinKey, dbMetaMaxKey, func(key, value []byte) (bool, error) {
		id, suffix, err := decodeMetaKey(key)
		if err != nil {
			return false, err
		}

		if suffix != dbStateSuffix {
			return true, nil
		}

		totalCount++
		localState := new(raftpb.DBLocalState)
		pbutil.MustUnmarshal(localState, value)
		for _, p := range localState.DB.Peers {
			s.addPeerToCache(*p)
		}

		if localState.State == raftpb.Tombstone {
			s.clearMeta(id, wb)
			tomebstoneCount++
			log.Infof("raftstore: db %d is tombstone in store",
				id)
			return true, nil
		}

		states = append(states, localState)
		return true, nil
	})

	if err != nil {
		log.Fatalf("raftstore: init store failed, errors:\n %+v", err)
	}
	err = s.metaStore.Write(wb, false)
	if err != nil {
		log.Fatalf("raftstore: init store write failed, errors:\n %+v", err)
	}

	var wg sync.WaitGroup
	for i := len(states) - 1; i >= 0; i-- {
		state := states[i]
		wg.Add(1)

		f := func(state *raftpb.DBLocalState) {
			defer wg.Done()

			db := &state.DB
			pr, err := createPeerReplicate(s, db)
			if err != nil {
				log.Fatalf("raftstore: init store failed, errors:\n %+v", err)
			}

			if state.State == raftpb.Applying {
				applyingCount++
				log.Infof("raftstore[db-%d]: applying in store", db.ID)
				pr.startApplySnapJob()
			}

			pr.startRegistrationJob()
			s.replicates.Store(db.ID, pr)
		}

		if i == len(states)-1 {
			f(state)
			atomic.StoreUint64(&s.firstDBLoaded, 1)
		} else {
			go f(state)
		}
	}

	wg.Wait()

	log.Infof("raftstore: starts with %d dbs, including %d tombstones and %d applying dbs",
		totalCount,
		tomebstoneCount,
		applyingCount)
}

func (s *Store) startRaftCompact() {
	s.runner.RunCancelableTask(func(ctx context.Context) {
		ticker := time.NewTicker(s.cfg.RaftLogCompactDuration)
		defer ticker.Stop()

		for {
			select {
			case <-ctx.Done():
				log.Infof("raftstore: raft log compact job stopped")
				return
			case <-ticker.C:
				s.replicates.Range(func(key, value interface{}) bool {
					pr := value.(*PeerReplicate)
					if pr.isLeader() {
						pr.addAction(checkCompactAction)
					}

					return true
				})
			}
		}
	})
}

func (s *Store) clearMeta(id uint64, wb storage.WriteBatch) error {
	metaCount := 0
	raftCount := 0

	var keys [][]byte
	defer func() {
		for _, key := range keys {
			s.metaStore.Free(key)
		}
	}()

	// meta must in the range [id, id + 1)
	metaStart := getDBMetaPrefix(id)
	metaEnd := getDBMetaPrefix(id + 1)

	err := s.metaStore.ScanWithPooledKey(metaStart, metaEnd, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		err := wb.Delete(key)
		if err != nil {
			return false, err
		}

		metaCount++
		return true, nil
	}, true)
	if err != nil {
		return err
	}

	raftStart := getDBRaftPrefix(id)
	raftEnd := getDBRaftPrefix(id + 1)
	err = s.metaStore.ScanWithPooledKey(raftStart, raftEnd, func(key, value []byte) (bool, error) {
		keys = append(keys, key)
		err := wb.Delete(key)
		if err != nil {
			return false, err
		}

		raftCount++
		return true, nil
	}, true)
	if err != nil {
		return err
	}

	log.Infof("raftstore[db-%d]: clear peer %d meta keys and %d raft keys",
		id,
		metaCount,
		raftCount)
	return nil
}

func (s *Store) removePendingSnap(id uint64) {
	s.pendingSnaps.Delete(id)
}

func (s *Store) notify(n interface{}) {
	if msg, ok := n.(*raftpb.RaftMessage); ok {
		s.onRaftMessage(msg)
		util.ReleaseRaftMessage(msg)
	} else if msg, ok := n.(*raftpb.SnapshotMessage); ok {
		s.onSnapshotMessage(msg)
	}
}

func (s *Store) onSnapshotMessage(msg *raftpb.SnapshotMessage) {
	if msg.Chunk != nil {
		s.onSnapshotChunk(msg)
	}
}

func (s *Store) onSnapshotChunk(msg *raftpb.SnapshotMessage) {
	s.addApplyJob(msg.Header.DB.ID, "onSnapshotData", func() error {
		err := s.snapMgr.ReceiveSnapData(msg)
		if err != nil {
			log.Fatalf("raftstore[db-%d]: received snap data failed, errors:\n%+v",
				msg.Header.DB.ID,
				err)
		}

		return nil
	}, nil)
}

func (s *Store) onRaftMessage(msg *raftpb.RaftMessage) {
	if !s.isRaftMsgValid(msg) {
		return
	}

	if msg.Tombstone {
		// we receive a message tells us to remove ourself.
		s.handleGCPeerMsg(msg)
		return
	}

	yes, err := s.isMsgStale(msg)
	if err != nil || yes {
		return
	}

	if !s.tryToCreatePeerReplicate(msg.ID, msg) {
		return
	}

	s.addPeerToCache(msg.From)
	pr := s.getDB(msg.ID, false)
	pr.step(msg.Message)
}

// If target peer doesn't exist, create it.
//
// return false to indicate that target peer is in invalid state or
// doesn't exist and can't be created.
func (s *Store) tryToCreatePeerReplicate(id uint64, msg *raftpb.RaftMessage) bool {
	var (
		hasPeer     = false
		asyncRemove = false
		stalePeer   meta.Peer
	)

	target := msg.To
	if p := s.getDB(id, false); p != nil {
		hasPeer = true

		// we may encounter a message with larger peer id, which means
		// current peer is stale, then we should remove current peer
		if p.peer.ID < target.ID {
			// cancel snapshotting op
			if p.ps.isApplyingSnap() && !p.ps.cancelApplySnapJob() {
				log.Infof("raftstore[db-%d]: stale peer is applying snapshot, will destroy next time, peer=<%d>",
					id,
					p.peer.ID)

				return false
			}

			stalePeer = p.peer
			asyncRemove = p.ps.isInitialized()
		} else if p.peer.ID > target.ID {
			log.Infof("raftstore[db-%d]: may be from peer is stale, targetID=<%d> currentID=<%d>",
				id,
				target.ID,
				p.peer.ID)
			return false
		}
	}

	// If we found stale peer, we will destory it
	if stalePeer.ID > 0 {
		s.destroyPeer(id, stalePeer, asyncRemove)
		if asyncRemove {
			return false
		}

		hasPeer = false
	}

	if hasPeer {
		return true
	}

	// arrive here means target peer not found, we will try to create it
	message := msg.Message
	if message.Type != etcdraftpb.MsgVote &&
		(message.Type != etcdraftpb.MsgHeartbeat || message.Commit != 0) {
		log.Infof("raftstore[db-%d]: target peer doesn't exist, peer=<%+v> message=<%s>",
			id,
			target,
			message.Type)
		return false
	}

	// if the newest db in the store is not loaded, ignore
	if atomic.LoadUint64(&s.firstDBLoaded) == 0 {
		log.Infof("raftstore[db-%d]: ignore create peer, wait first db loaded to decide",
			id)
		return false
	}

	// check range overlapped
	// if we have the writeable db, and has overlapped with new db, we cann't create the replicate
	pr := s.getWriteableDB(false)
	if pr != nil && pr.ps.db.Start+s.cfg.MaxDBRecords <= msg.Start {
		log.Infof("raftstore[db-%d]: overlapped with %d, [%d,%d)",
			pr.id,
			msg.Start,
			pr.ps.db.Start,
			msg.Start)
		return false
	}

	// now we can create a replicate
	// the create replicate will not start the mq consumer now
	// it will started at after apply the snapshot
	peerReplicate, err := doReplicate(s, msg, target.ID)
	if err != nil {
		log.Errorf("raftstore[db-%d]: replicate peer failure, errors:\n %+v",
			id,
			err)
		return false
	}

	peerReplicate.ps.db.Peers = append(peerReplicate.ps.db.Peers, &msg.To)
	peerReplicate.ps.db.Peers = append(peerReplicate.ps.db.Peers, &msg.From)

	// following snapshot may overlap, should insert into keyRanges after
	// snapshot is applied.
	s.replicates.Store(id, peerReplicate)
	s.addPeerToCache(msg.From)
	s.addPeerToCache(msg.To)

	log.Infof("raftstore[db-%d]: created by raft msg: %+v",
		peerReplicate.id,
		msg)
	return true
}

func (s *Store) isRaftMsgValid(msg *raftpb.RaftMessage) bool {
	if msg.To.StoreID != s.meta.ID {
		log.Warnf("raftstore[store-%d]: store not match, toPeerStoreID=<%d> mineStoreID=<%d>",
			s.meta.ID,
			msg.To.StoreID,
			s.meta.ID)
		return false
	}

	return true
}

func (s *Store) isMsgStale(msg *raftpb.RaftMessage) (bool, error) {
	fromEpoch := msg.Epoch
	isVoteMsg := msg.Message.Type == etcdraftpb.MsgVote
	fromStoreID := msg.From.StoreID

	// Let's consider following cases with three nodes [1, 2, 3] and 1 is leader:
	// a. 1 removes 2, 2 may still send MsgAppendResponse to 1.
	//  We should ignore this stale message and let 2 remove itself after
	//  applying the ConfChange log.
	// b. 2 is isolated, 1 removes 2. When 2 rejoins the cluster, 2 will
	//  send stale MsgRequestVote to 1 and 3, at this time, we should tell 2 to gc itself.
	// c. 2 is isolated but can communicate with 3. 1 removes 3.
	//  2 will send stale MsgRequestVote to 3, 3 should ignore this message.
	// d. 2 is isolated but can communicate with 3. 1 removes 2, then adds 4, remove 3.
	//  2 will send stale MsgRequestVote to 3, 3 should tell 2 to gc itself.
	// e. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader.
	//  After 2 rejoins the cluster, 2 may send stale MsgRequestVote to 1 and 3,
	//  1 and 3 will ignore this message. Later 4 will send messages to 2 and 2 will
	//  rejoin the raft group again.
	// f. 2 is isolated. 1 adds 4, 5, 6, removes 3, 1. Now assume 4 is leader, and 4 removes 2.
	//  unlike case e, 2 will be stale forever.
	// TODO: for case f, if 2 is stale for a long time, 2 will communicate with pd and pd will
	// tell 2 is stale, so 2 can remove itself.
	pr := s.getDB(msg.ID, false)
	if nil != pr {
		db := pr.ps.db
		epoch := db.Epoch
		if isEpochStale(fromEpoch, epoch) &&
			findPeer(&db, fromStoreID) == nil {
			s.handleStaleMsg(msg, epoch, isVoteMsg)
			return true, nil
		}

		return false, nil
	}

	// no exist, check with tombstone key.
	localState, err := loadDBRaftLocalState(msg.ID, s.metaStore, true)
	if err != nil {
		return false, err
	}

	if localState != nil {
		if localState.State != raftpb.Tombstone {
			// Maybe split, but not registered yet.
			s.cacheDroppedVoteMsg(msg.ID, msg.Message)
			return false, fmt.Errorf("db<%d> not exist but not tombstone, local state: %s",
				msg.ID,
				localState.String())
		}

		dbEpoch := localState.DB.Epoch

		// The cell in this peer is already destroyed
		if isEpochStale(fromEpoch, dbEpoch) {
			log.Infof("raftstore[db-%d]: tombstone peer receive a a stale message, epoch=<%s> msg=<%s>",
				msg.ID,
				dbEpoch.String(),
				msg.String())
			notExist := findPeer(&localState.DB, fromStoreID) == nil
			s.handleStaleMsg(msg, dbEpoch, isVoteMsg && notExist)

			return true, nil
		}

		if fromEpoch.ConfVersion == dbEpoch.ConfVersion {
			return false, fmt.Errorf("tombstone peer receive an invalid message, epoch=<%s> msg=<%s>",
				dbEpoch.String(),
				msg.String())

		}
	}

	return false, nil
}

func (s *Store) handleGCPeerMsg(msg *raftpb.RaftMessage) {
	needRemove := false
	asyncRemoved := true

	pr := s.getDB(msg.ID, false)
	if pr != nil {
		fromEpoch := msg.Epoch
		if isEpochStale(pr.ps.db.Epoch, fromEpoch) {
			log.Infof("raftstore[db-%d]: receives gc message, remove. msg=<%+v>",
				msg.ID,
				msg)
			needRemove = true
			asyncRemoved = pr.ps.isInitialized()
		}
	}

	if needRemove {
		s.destroyPeer(msg.ID, msg.To, asyncRemoved)
	}
}

func (s *Store) handleStaleMsg(msg *raftpb.RaftMessage, currEpoch meta.Epoch, needGC bool) {
	fromPeer := msg.From
	toPeer := msg.To

	if !needGC {
		log.Infof("raftstore[db-%d]: raft msg is stale, ignore it, msg=<%+v> current=<%+v>",
			msg.ID,
			msg,
			currEpoch)
		return
	}

	log.Infof("raftstore[db-%d]: raft msg is stale, tell to gc, msg=<%+v> current=<%+v>",
		msg.ID,
		msg,
		currEpoch)

	gc := new(raftpb.RaftMessage)
	gc.ID = msg.ID
	gc.To = fromPeer
	gc.From = toPeer
	gc.Epoch = currEpoch
	gc.Tombstone = true
	s.trans.sendRaftMessage(gc)
}

func (s *Store) destroyPeer(id uint64, target meta.Peer, async bool) {
	if !async {
		log.Infof("raftstore[db-%d]: destroying stale peer, peer=<%v>",
			id,
			target)

		pr := s.getDB(id, false)
		if pr == nil {
			log.Fatalf("raftstore[db-%d]: destroy db not exist", id)
		}

		if pr.ps.isApplyingSnap() {
			log.Fatalf("raftstore[db-%d]: destroy db is apply for snapshot", id)
		}

		err := pr.destroy()
		if err != nil {
			log.Fatalf("raftstore[db-%d]: destroy db failed, errors:\n %+v",
				id,
				err)
		}
	} else {
		log.Infof("raftstore[db-%d]: asking destroying stale peer, peer=<%v>",
			id,
			target)
		s.startDestroyJob(id, target)
	}
}

// In some case, the vote raft msg maybe dropped, so follwer node can't response the vote msg
// DB a has 3 peers p1, p2, p3. The p1 split to new DB b
// case 1: in most sence, p1 apply split raft log is before p2 and p3.
//         At this time, if p2, p3 received the DB b's vote msg,
//         and this vote will dropped by p2 and p3 node,
//         because DB a and DB b has overlapped range at p2 and p3 node
// case 2: p2 or p3 apply split log is before p1, we can't mock DB b's vote msg
func (s *Store) cacheDroppedVoteMsg(id uint64, msg etcdraftpb.Message) {
	if msg.Type == etcdraftpb.MsgVote {
		s.droppedVoteMsgs.Store(id, msg)
	}
}

func (s *Store) removeDroppedVoteMsg(id uint64) (etcdraftpb.Message, bool) {
	if value, ok := s.droppedVoteMsgs.Load(id); ok {
		s.droppedVoteMsgs.Delete(id)
		return value.(etcdraftpb.Message), true
	}

	return etcdraftpb.Message{}, false
}

func (s *Store) addPeerToCache(peer meta.Peer) {
	s.peers.Store(peer.ID, peer)
}

func (s *Store) getWriteableDB(leader bool) *PeerReplicate {
	var pr *PeerReplicate
	s.replicates.Range(func(key, value interface{}) bool {
		p := value.(*PeerReplicate)
		if p.isWritable() &&
			(!leader ||
				(leader && p.isLeader())) {
			pr = p
			return false
		}

		return true
	})

	return pr
}

func (s *Store) getDB(id uint64, leader bool) *PeerReplicate {
	if pr, ok := s.replicates.Load(id); ok {
		p := pr.(*PeerReplicate)
		if !leader ||
			(leader && p.isLeader()) {
			return p
		}

		return nil
	}

	return nil
}

func (s *Store) getPeer(id uint64) *meta.Peer {
	if p, ok := s.peers.Load(id); ok {
		return p.(*meta.Peer)
	}

	return nil
}

type dbStatus struct {
	dbCount, dbLeaderCount, applyingSnapCount uint64
}

func (s *Store) getDBStatus() dbStatus {
	st := dbStatus{}
	s.replicates.Range(func(key, value interface{}) bool {
		pr := value.(*PeerReplicate)
		st.dbCount++
		if pr.ps.isApplyingSnap() {
			st.applyingSnapCount++
		}

		if pr.isLeader() {
			st.dbLeaderCount++
		}

		return true
	})

	return st
}

func (s *Store) validateStoreID(req *raftpb.RaftCMDRequest) error {
	if req.Header.Peer.StoreID != s.meta.ID {
		return fmt.Errorf("store not match, give=<%d> want=<%d>",
			req.Header.Peer.StoreID,
			s.meta.ID)
	}

	return nil
}

func (s *Store) validateDB(req *raftpb.RaftCMDRequest) *raftpb.Error {
	dbID := req.Header.ID
	peerID := req.Header.Peer.ID

	pr := s.getDB(dbID, false)
	if nil == pr {
		return errorDBNotFound(dbID)
	}

	if !pr.isLeader() {
		return errorNotLeader(dbID, s.getPeer(pr.leaderPeerID()))
	}

	if pr.peer.ID != peerID {
		return errorOtherCMDResp(fmt.Errorf("mismatch peer id, give=<%d> want=<%d>", peerID, pr.peer.ID))
	}

	// If header's term is 2 verions behind current term,
	// leadership may have been changed away.
	if req.Header.Term > 0 && pr.getCurrentTerm() > req.Header.Term+1 {
		return errorStaleCMDResp()
	}

	if !checkEpoch(pr.ps.db, req) {
		wp := s.getWriteableDB(true)
		if wp != nil {
			return errorStaleEpochResp(wp.ps.db)
		}

		return errorStaleEpochResp()
	}

	return nil
}
