package raftstore

import (
	"fmt"
	"sync"
	"time"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/coreos/etcd/raft"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/prophet"
)

type reqCtx struct {
	msgType    int32
	admin      *raftpb.AdminRequest
	search     *rpc.SearchRequest
	insert     *rpc.InsertRequest
	update     *rpc.UpdateRequest
	cb         func(interface{})
	searchNext bool
}

func (r *reqCtx) reset() {
	r.admin = nil
	r.insert = nil
	r.update = nil
	r.cb = nil
	r.msgType = -1
	r.searchNext = false
}

// PeerReplicate is the db's peer replicate. Every db replicate has a PeerReplicate.
type PeerReplicate struct {
	id       uint64
	peer     meta.Peer
	store    *Store
	ps       *peerStorage
	batching *batching

	rn               *raft.RawNode
	events           *util.RingBuffer
	ticks            *util.Queue
	steps            *util.Queue
	reports          *util.Queue
	applyResults     *util.Queue
	requests         *util.Queue
	searches         *util.Queue
	mqRequests       *util.Queue
	mqUpdateRequests *util.Queue
	actions          *util.Queue
	stopRaftTick     bool
	raftLogSizeHint  uint64

	heartbeatsMap *sync.Map

	cancelTaskIds []uint64

	consumerCloseOnce, consumerStartOnce sync.Once
	consumer                             *cluster.Consumer
	condL                                *sync.Mutex
	cond                                 *sync.Cond

	alreadySplit bool
}

func createPeerReplicate(store *Store, db *meta.VectorDB) (*PeerReplicate, error) {
	peer := findPeer(db, store.meta.ID)
	if peer == nil {
		return nil, fmt.Errorf("find no peer for store %d in db %v",
			store.meta.ID,
			db)
	}

	return newPeerReplicate(store, db, peer.ID)
}

// The peer can be created from another node with raft membership changes, and we only
// know the db_id and peer_id when creating this replicated peer, the db info
// will be retrieved later after applying snapshot.
func doReplicate(store *Store, msg *raftpb.RaftMessage, peerID uint64) (*PeerReplicate, error) {
	// We will remove tombstone key when apply snapshot
	log.Infof("raftstore[db-%d]: replicate peer %d",
		msg.ID,
		peerID)

	db := &meta.VectorDB{
		ID:    msg.ID,
		Epoch: msg.Epoch,
		State: msg.DBState,
	}

	return newPeerReplicate(store, db, peerID)
}

func newPeerReplicate(store *Store, db *meta.VectorDB, peerID uint64) (*PeerReplicate, error) {
	if peerID == 0 {
		return nil, fmt.Errorf("invalid peer id %d", peerID)
	}

	ps, err := newPeerStorage(store, *db)
	if err != nil {
		return nil, err
	}

	pr := new(PeerReplicate)
	pr.id = db.ID
	pr.peer = newPeer(peerID, store.meta.ID)
	pr.ps = ps
	pr.store = store
	pr.events = util.NewRingBuffer(2)
	pr.ticks = util.New(0)
	pr.steps = util.New(0)
	pr.reports = util.New(0)
	pr.applyResults = util.New(0)
	pr.requests = util.New(0)
	pr.searches = util.New(0)
	pr.mqRequests = util.New(0)
	pr.actions = util.New(0)
	pr.mqUpdateRequests = util.New(0)
	pr.heartbeatsMap = &sync.Map{}
	pr.batching = newBatching(pr)
	pr.condL = &sync.Mutex{}
	pr.cond = sync.NewCond(pr.condL)

	c := store.cfg.getRaftConfig(peerID, ps.appliedIndex(), ps)
	rn, err := raft.NewRawNode(c, nil)
	if err != nil {
		return nil, err
	}
	pr.rn = rn

	// If this db has only one peer and I am the one, campaign directly.
	if len(db.Peers) == 1 && db.Peers[0].StoreID == store.meta.ID {
		err = rn.Campaign()
		if err != nil {
			return nil, err
		}

		log.Debugf("raftstore[db-%d]: try to campaign leader",
			pr.id)
	}

	id, _ := store.runner.RunCancelableTask(pr.readyToServeRaft)
	pr.cancelTaskIds = append(pr.cancelTaskIds, id)

	id, _ = store.runner.RunCancelableTask(pr.asyncExecUpdates)
	pr.cancelTaskIds = append(pr.cancelTaskIds, id)

	id, _ = store.runner.RunCancelableTask(ps.rebuildIndex)
	pr.cancelTaskIds = append(pr.cancelTaskIds, id)

	return pr, nil
}

func (pr *PeerReplicate) maybeCampaign() (bool, error) {
	if len(pr.ps.db.Peers) <= 1 {
		// The peer campaigned when it was created, no need to do it again.
		return false, nil
	}

	err := pr.rn.Campaign()
	if err != nil {
		return false, err
	}

	log.Debugf("raftstore[db-%d]: try to campaign leader",
		pr.id)
	return true, nil
}

func (pr *PeerReplicate) tryCampaign() error {
	// If this db has only one peer and I am the one, campaign directly.
	if len(pr.ps.db.Peers) == 1 && pr.ps.db.Peers[0].StoreID == pr.store.meta.ID {
		err := pr.rn.Campaign()
		if err != nil {
			return err
		}

		log.Debugf("raftstore[db-%d]: try to campaign leader",
			pr.id)
	}

	return nil
}

func (pr *PeerReplicate) stopEventLoop() {
	pr.events.Dispose()
	pr.closeAllQueues()
}

func (pr *PeerReplicate) closeAllQueues() {
	pr.ticks.Dispose()
	pr.steps.Dispose()
	pr.reports.Dispose()
	pr.applyResults.Dispose()
	pr.requests.Dispose()
	pr.mqRequests.Dispose()
	pr.mqUpdateRequests.Dispose()
	pr.actions.Dispose()
}

func (pr *PeerReplicate) destroy() error {
	log.Infof("raftstore[db-%d]: begin to destroy",
		pr.id)

	pr.maybeStopConsumer()
	pr.stopEventLoop()
	pr.store.removeDroppedVoteMsg(pr.id)
	wb := pr.store.metaStore.NewWriteBatch()
	err := pr.store.clearMeta(pr.id, wb)
	if err != nil {
		return err
	}

	err = pr.ps.updatePeerState(pr.ps.db, raftpb.Tombstone, wb)
	if err != nil {
		return err
	}

	err = pr.store.metaStore.Write(wb, false)
	if err != nil {
		return err
	}

	if pr.ps.isInitialized() {
		err := pr.ps.clearData()
		if err != nil {
			log.Errorf("raftstore[db-%d]: add clear data failed, errors:\n %+v",
				pr.id,
				err)
			return err
		}
	}

	for _, id := range pr.cancelTaskIds {
		pr.store.runner.StopCancelableTask(id)
	}

	pr.ps.destroy()
	log.Infof("raftstore-[db-%d]: destroy self complete.",
		pr.id)
	return nil
}

func (pr *PeerReplicate) checkPeers() {
	if !pr.isLeader() {
		pr.heartbeatsMap.Range(func(key, value interface{}) bool {
			pr.heartbeatsMap.Delete(key)
			return true
		})
		return
	}

	peers := pr.ps.db.Peers
	// Insert heartbeats in case that some peers never response heartbeats.
	for _, p := range peers {
		pr.heartbeatsMap.LoadOrStore(p.ID, time.Now())
	}
}

func (pr *PeerReplicate) collectDownPeers(maxDuration time.Duration) []*prophet.PeerStats {
	now := time.Now()
	var downPeers []*prophet.PeerStats
	for _, p := range pr.ps.db.Peers {
		if p.ID == pr.peer.ID {
			continue
		}

		if last, ok := pr.heartbeatsMap.Load(p.ID); ok {
			missing := now.Sub(last.(time.Time))
			if missing >= maxDuration {
				state := &prophet.PeerStats{}
				state.Peer = &prophet.Peer{ID: p.ID, ContainerID: p.StoreID}
				state.DownSeconds = uint64(missing.Seconds())

				downPeers = append(downPeers, state)
			}
		}
	}
	return downPeers
}

func (pr *PeerReplicate) collectPendingPeers() []*prophet.Peer {
	var pendingPeers []*prophet.Peer
	status := pr.rn.Status()
	truncatedIdx := pr.ps.truncatedIndex()

	for id, progress := range status.Progress {
		if id == pr.peer.ID {
			continue
		}

		if progress.Match < truncatedIdx {
			if p := pr.store.getPeer(id); p != nil {
				pendingPeers = append(pendingPeers, &prophet.Peer{ID: p.ID, ContainerID: p.StoreID})
			}
		}
	}

	return pendingPeers
}

func (pr *PeerReplicate) getCurrentTerm() uint64 {
	return pr.rn.Status().Term
}

func (pr *PeerReplicate) onAdmin(req *raftpb.AdminRequest) error {
	r := acquireReqCtx()
	r.msgType = int32(rpc.MsgAdmin)
	r.admin = req
	return pr.addRequest(r)
}

func (pr *PeerReplicate) onAdminWithCB(req *raftpb.AdminRequest, cb func(interface{})) error {
	r := acquireReqCtx()
	r.msgType = int32(rpc.MsgAdmin)
	r.admin = req
	r.cb = cb
	err := pr.addRequest(r)
	if err != nil {
		cb(err)
	}
	return err
}

func (pr *PeerReplicate) onInsert(req *rpc.InsertRequest, cb func(interface{})) {
	r := acquireReqCtx()
	r.msgType = int32(rpc.MsgInsertReq)
	r.insert = req
	r.cb = cb
	err := pr.addRequest(r)
	if err != nil && cb != nil {
		cb(errorStaleCMDResp((req.ID)))
		util.ReleaseInsertReq(req)
	}
}

func (pr *PeerReplicate) onUpdate(req *rpc.UpdateRequest, cb func(interface{})) {
	r := acquireReqCtx()
	r.msgType = int32(rpc.MsgUpdateReq)
	r.update = req
	r.cb = cb
	err := pr.addRequest(r)
	if err != nil && cb != nil {
		cb(errorStaleCMDResp(req.ID))
		util.ReleaseUpdateReq(req)
	}
}

func (pr *PeerReplicate) waitInsertCommitted(req *rpc.SearchRequest) bool {
	if pr.isWritable() {
		pr.condL.Lock()
		for {
			offset, index := pr.ps.committedOffset()
			requestOffsetIsBigger := req.Offset > offset || (req.Offset == offset && index != indexComplete)

			log.Debugf("raftstore[db-%d]: search with offset %d, current offset %d index %d",
				req.DB,
				req.Offset,
				offset,
				index)

			// this db is not writable, if the request offset is bigger than the lastest committed offset,
			// the client need search the new writable range
			if pr.ps.availableWriteRecords() <= 0 &&
				requestOffsetIsBigger {
				pr.condL.Unlock()
				return req.Last
			}

			if requestOffsetIsBigger {
				pr.cond.Wait()
			} else {
				break
			}
		}
		pr.condL.Unlock()
	} else {
		offset, index := pr.ps.committedOffset()
		requestOffsetIsBigger := req.Offset > offset || (req.Offset == offset && index != indexComplete)
		if requestOffsetIsBigger && req.Last {
			return true
		}
	}

	return false
}
