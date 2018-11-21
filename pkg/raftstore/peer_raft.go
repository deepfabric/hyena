package raftstore

import (
	"context"
	"fmt"
	"time"

	etcdraft "github.com/coreos/etcd/raft"
	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

type requestPolicy int

const (
	proposeNormal         = requestPolicy(1)
	proposeTransferLeader = requestPolicy(2)
	proposeChange         = requestPolicy(3)
)

var (
	emptyStruct                         = struct{}{}
	batch                        int64  = 32
	maxTransferLeaderAllowLogLag uint64 = 10
)

type action int

const (
	checkCompactAction = iota
	checkSplitAction
	doCampaignAction
)

func (pr *PeerReplicate) isLeader() bool {
	return pr.rn.Status().RaftState == etcdraft.StateLeader
}

func (pr *PeerReplicate) isWritable() bool {
	return pr.ps.db.State == meta.RWU
}

func (pr *PeerReplicate) leaderPeerID() uint64 {
	return pr.rn.Status().Lead
}

func (pr *PeerReplicate) nextProposalIndex() uint64 {
	return pr.rn.NextProposalIndex()
}

func (pr *PeerReplicate) step(msg etcdraftpb.Message) {
	err := pr.steps.Put(msg)
	if err != nil {
		log.Infof("raftstore[db-%d]: raft step stopped",
			pr.id)
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) report(report interface{}) {
	err := pr.reports.Put(report)
	if err != nil {
		log.Infof("raftstore[db-%d]: raft report stopped",
			pr.id)
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) addRequest(req *reqCtx) error {
	err := pr.requests.Put(req)
	if err != nil {
		return err
	}

	pr.addEvent()
	return nil
}

func (pr *PeerReplicate) addSearchRequest(req *reqCtx) error {
	err := pr.searches.Put(req)
	if err != nil {
		return err
	}

	pr.addEvent()
	return nil
}

func (pr *PeerReplicate) addRequestFromMQ(req interface{}) error {
	err := pr.mqRequests.Put(req)
	if err != nil {
		return err
	}

	pr.addEvent()
	return nil
}

func (pr *PeerReplicate) addApplyResult(result *asyncApplyResult) {
	err := pr.applyResults.Put(result)
	if err != nil {
		log.Infof("raftstore[db-%d]: raft apply result stopped",
			pr.id)
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) addAction(act action) {
	err := pr.actions.Put(act)
	if err != nil {
		return
	}

	pr.addEvent()
}

func (pr *PeerReplicate) addEvent() (bool, error) {
	return pr.events.Offer(emptyStruct)
}

func (pr *PeerReplicate) onRaftTick(arg interface{}) {
	if !pr.stopRaftTick {
		err := pr.ticks.Put(emptyStruct)
		if err != nil {
			log.Infof("raftstore[db-%d]: raft tick stopped",
				pr.id)
			return
		}

		pr.addEvent()
	}

	util.DefaultTimeoutWheel().Schedule(pr.store.cfg.RaftTickDuration, pr.onRaftTick, nil)
}

func (pr *PeerReplicate) readyToServeRaft(ctx context.Context) {
	pr.onRaftTick(nil)
	items := make([]interface{}, batch, batch)

	for {
		if pr.events.Len() == 0 && !pr.events.IsDisposed() {
			time.Sleep(time.Millisecond * 10)
			continue
		}

		_, err := pr.events.Get()
		if err != nil {
			pr.handleStop()
			log.Infof("raftstore[db-%d]: handle serve raft stopped",
				pr.id)
			return
		}

		pr.handleAction(items)
		pr.handleTick(items)
		pr.handleReport(items)
		pr.handleRequest(items)
		pr.handleSearch(items)
		pr.handleRequestFromMQ(items)

		pr.handleApplyResult(items)
		pr.handleStep(items)
		if pr.rn.HasReadySince(pr.ps.lastReadyIndex) {
			pr.handleReady()
		}
	}
}

func (pr *PeerReplicate) handleStop() {
	pr.actions.Dispose()
	pr.ticks.Dispose()
	pr.steps.Dispose()
	pr.reports.Dispose()

	results := pr.applyResults.Dispose()
	for _, result := range results {
		releaseAsyncApplyResult(result.(*asyncApplyResult))
	}

	if pr.isWritable() && pr.cond != nil {
		pr.cond.Broadcast()
	}

	// resp all stale requests in batch and queue
	searches := pr.searches.Dispose()
	for _, r := range searches {
		c := r.(*reqCtx)
		c.cb(errorStaleCMDResp(c.search.ID))
		releaseReqCtx(c)
	}

	requests := pr.requests.Dispose()
	for _, r := range requests {
		req := r.(*reqCtx)
		pr.batching.put(req)
	}

	for {
		if pr.batching.isEmpty() {
			break
		}
		c := pr.batching.pop()
		c.respError(errorStaleCMDResp(nil))
	}
}

func (pr *PeerReplicate) handleAction(items []interface{}) {
	size := pr.actions.Len()
	if size == 0 {
		return
	}

	n, err := pr.actions.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		a := items[i].(action)
		switch a {
		case checkCompactAction:
			pr.handleCheckCompact()
		case checkSplitAction:
			pr.maybeSplit()
		case doCampaignAction:
			_, err := pr.maybeCampaign()
			if err != nil {
				log.Fatalf("raftstore[db-%d]: campaign failed, errors:\n %+v",
					pr.id,
					err)
			}
		}
	}

	if pr.actions.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleStep(items []interface{}) {
	size := pr.steps.Len()
	if size == 0 {
		return
	}

	n, err := pr.steps.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		msg := items[i].(etcdraftpb.Message)
		if pr.isLeader() && msg.From != 0 {
			pr.heartbeatsMap.Store(msg.From, time.Now())
		}

		err := pr.rn.Step(msg)
		if err != nil {
			log.Errorf("[db-%d]: step failed, error:\n%+v",
				pr.id,
				err)
		}

		if len(msg.Entries) > 0 {
			log.Debugf("raftstore[db-%d]: step raft with %d entries",
				pr.id,
				len(msg.Entries))
		}
	}

	if pr.steps.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleTick(items []interface{}) {
	size := pr.ticks.Len()
	if size == 0 {
		return
	}

	n, err := pr.ticks.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		if !pr.stopRaftTick {
			pr.rn.Tick()
		}
	}

	if pr.ticks.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleReport(items []interface{}) {
	size := pr.reports.Len()
	if size == 0 {
		return
	}

	n, err := pr.reports.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		if msg, ok := items[i].(etcdraftpb.Message); ok {
			pr.rn.ReportUnreachable(msg.To)
			if msg.Type == etcdraftpb.MsgSnap {
				pr.rn.ReportSnapshot(msg.To, etcdraft.SnapshotFailure)
			}
		}
	}

	if pr.reports.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleApplyResult(items []interface{}) {
	for {
		size := pr.applyResults.Len()
		if size == 0 {
			break
		}

		n, err := pr.applyResults.Get(batch, items)
		if err != nil {
			return
		}

		for i := int64(0); i < n; i++ {
			result := items[i].(*asyncApplyResult)
			pr.doPollApply(result)
			releaseAsyncApplyResult(result)
		}

		if n < batch {
			break
		}
	}
}

func (pr *PeerReplicate) handleRequest(items []interface{}) {
	size := pr.requests.Len()
	if size == 0 {
		return
	}

	n, err := pr.requests.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		req := items[i].(*reqCtx)
		pr.batching.put(req)
	}

	for {
		if pr.batching.isEmpty() {
			break
		}

		pr.propose(pr.batching.pop())
	}

	if pr.requests.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleSearch(items []interface{}) {
	size := pr.searches.Len()
	if size == 0 {
		return
	}

	n, err := pr.searches.Get(batch, items)
	if err != nil {
		return
	}

	for i := int64(0); i < n; i++ {
		c := items[i].(*reqCtx)
		pr.execSearch(c.search, c.cb, c.searchNext)
		releaseReqCtx(c)
	}

	if pr.searches.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleRequestFromMQ(items []interface{}) {
	if pr.ps.availableWriteRecords() == 0 {
		pr.maybeStopConsumer()
		return
	}

	size := pr.mqRequests.Len()
	if size == 0 {
		return
	}

	// If we are create snapshot, skip handle request
	if size > 0 && pr.ps.genSnapJob != nil && pr.ps.genSnapJob.IsNotComplete() {
		pr.addEvent()
		return
	}

	n, err := pr.mqRequests.Get(batch, items)
	if err != nil {
		return
	}

	from, fromIndex := pr.ps.committedOffset()
	vBatch := acquireVdbBatch()
	vBatch.available = pr.ps.availableWriteRecords()

	// 1. batch added to local vectordb
	committedOffset := int64(0)
	index := int64(0)

	for i := int64(0); i < n; i++ {
		req := items[i].(*rpc.InsertRequest)
		if vBatch.appendable() {
			if req.Offset > from {
				committedOffset = req.Offset
				index = vBatch.append(req.Xbs, req.Ids)
			} else if req.Offset == from && fromIndex != indexComplete {
				committedOffset = req.Offset
				index = vBatch.appendFromIndex(req.Xbs, req.Ids, fromIndex)
			}
		}

		util.ReleaseInsertReq(req)
	}

	// maybe mq client consumer stale message
	if len(vBatch.ids) == 0 {
		if pr.requests.Len() > 0 {
			pr.addEvent()
		}

		releaseVdbBatch(vBatch)
		return
	}

	pr.ps.vectorRecords += uint64(len(vBatch.ids))
	err = pr.ps.vdb.AddWithIds(vBatch.xbs, vBatch.ids)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: batch insert failed, errors:%+v",
			pr.id,
			err)
	}

	log.Debugf("raftstore[db-%d]: after added had %d records, last committed offset %d, index %d",
		pr.id,
		pr.ps.vectorRecords,
		committedOffset,
		index)

	releaseVdbBatch(vBatch)

	// 2. update the committed offset and notify all waitting search request
	pr.ps.setCommittedOffset(committedOffset, index)
	err = pr.store.metaStore.Set(getRaftApplyStateKey(pr.id), pbutil.MustMarshal(&pr.ps.raftApplyState), false)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: save apply context failed, errors:\n %+v",
			pr.id,
			err)
	}
	pr.cond.Broadcast()

	// 3. check split and stop the mq consumer, close the mqRequests queue,
	// so the old peer cann't be consumer new request
	pr.maybeSplit()

	if pr.requests.Len() > 0 {
		pr.addEvent()
	}
}

func (pr *PeerReplicate) handleReady() {
	// If we continue to handle all the messages, it may cause too many messages because
	// leader will send all the remaining messages to this follower, which can lead
	// to full message queue under high load.
	if pr.ps.isApplyingSnap() {
		log.Debugf("raftstore[db-%d]: still applying snapshot, skip further handling",
			pr.id)
		return
	}
	pr.ps.resetApplySnapJob()

	// wait apply committed entries complete
	if pr.rn.HasPendingSnapshot() &&
		!pr.ps.isApplyComplete() {
		log.Debugf("raftstore[db-%d]: apply index %d and committed index %d not match, skip applying snapshot",
			pr.id,
			pr.ps.appliedIndex(),
			pr.ps.committedIndex())
		return
	}

	rd := pr.rn.ReadySince(pr.ps.lastReadyIndex)
	log.Debugf("raftstore[db-%d]: raft ready after %d, %+v",
		pr.id,
		pr.ps.lastReadyIndex,
		rd)

	ctx := acquireReadyContext()
	// If snapshot is received, further handling
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		ctx.snap = &raftpb.SnapshotMessage{}
		pbutil.MustUnmarshal(ctx.snap, rd.Snapshot.Data)

		// When we apply snapshot, stop raft tick and resume until the snapshot applied.
		// If we still running raft tick, this peer will election timed out,
		// and sent vote msg to all peers.
		if !pr.stopRaftTick {
			pr.stopRaftTick = true
		}

		if !pr.store.snapMgr.Exists(ctx.snap) {
			log.Debugf("raftstore[db-%d]: receiving snapshot, skip further handling",
				pr.id)
			releaseReadyContext(ctx)
			return
		}
	}

	// If we become leader, send heartbeat to pd
	if rd.SoftState != nil {
		if rd.SoftState.RaftState == etcdraft.StateLeader {
			log.Infof("raftstore[db-%d]: ********become leader now********",
				pr.id)
			pr.store.pd.GetRPC().TiggerResourceHeartbeat(pr.id)
			log.Infof("raftstore[db-%d]: resource heartbeat trigger complete",
				pr.id)
			pr.resetBatching()
			pr.maybeSplit()
		} else {
			log.Infof("raftstore[db-%d]: ********become follower now********",
				pr.id)
		}

		// now we are join in the raft group, bootstrap the mq consumer to process insert requests
		if pr.isWritable() &&
			etcdraft.IsEmptySnap(rd.Snapshot) &&
			// create by raft message, need start mq after apply snapshot
			pr.ps.raftApplyState.AppliedIndex > 0 {
			pr.maybeStartConsumer("join raft-group")
		}
	}

	// The leader can write to disk and replicate to the followers concurrently
	// For more details, check raft thesis 10.2.1.
	if pr.isLeader() {
		pr.send(rd.Messages)
	}

	ctx.raftState = pr.ps.raftlocalState
	ctx.applyState = pr.ps.raftApplyState
	ctx.lastTerm = pr.ps.lastTerm
	ctx.wb = pr.store.metaStore.NewWriteBatch()

	pr.handleRaftReadySnapshot(ctx, rd)
	pr.handleRaftReadyAppend(ctx, rd)
	pr.handleRaftReadyApply(ctx, rd)

	releaseReadyContext(ctx)
}

func (pr *PeerReplicate) handleRaftReadySnapshot(ctx *readyContext, rd etcdraft.Ready) {
	if !etcdraft.IsEmptySnap(rd.Snapshot) {
		pr.store.reveivingSnapCount++

		err := pr.ps.doAppendSnap(ctx, rd.Snapshot)
		if err != nil {
			log.Fatalf("raftstore[db-%d]: handle raft ready failure, errors:\n %+v",
				pr.id,
				err)
		}
	}
}

func (pr *PeerReplicate) handleRaftReadyAppend(ctx *readyContext, rd etcdraft.Ready) {
	if len(rd.Entries) > 0 {
		err := pr.ps.doAppendEntries(ctx, rd.Entries)
		if err != nil {
			log.Fatalf("raftstore[db-%d]: handle raft ready failure, errors:\n %+v",
				pr.id,
				err)
		}
	}

	if ctx.raftState.LastIndex > 0 && !etcdraft.IsEmptyHardState(rd.HardState) {
		ctx.raftState.HardState = rd.HardState
	}

	pr.doSaveRaftState(ctx)
	pr.doSaveApplyState(ctx)
	err := pr.store.metaStore.Write(ctx.wb, pr.store.cfg.SyncWrite)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: handle raft ready failure, errors\n %+v",
			pr.id,
			err)
	}
}

func (pr *PeerReplicate) handleRaftReadyApply(ctx *readyContext, rd etcdraft.Ready) {
	if ctx.snap != nil {
		// When apply snapshot, there is no log applied and not compacted yet.
		pr.raftLogSizeHint = 0
	}

	result := pr.doApplySnap(ctx, rd)
	if !pr.isLeader() {
		pr.send(rd.Messages)
	}

	if result != nil {
		pr.startRegistrationJob()
	}

	pr.handleApplyCommittedEntries(rd)

	pr.rn.AdvanceAppend(rd)
	if result != nil {
		// Because we only handle raft ready when not applying snapshot, so following
		// line won't be called twice for the same snapshot.
		pr.rn.AdvanceApply(pr.ps.lastReadyIndex)
	}
}

func (pr *PeerReplicate) handleApplyCommittedEntries(rd etcdraft.Ready) {
	if pr.ps.isApplyingSnap() {
		pr.ps.lastReadyIndex = pr.ps.truncatedIndex()
	} else {
		for _, entry := range rd.CommittedEntries {
			pr.raftLogSizeHint += uint64(len(entry.Data))
		}

		if len(rd.CommittedEntries) > 0 {
			pr.ps.lastReadyIndex = rd.CommittedEntries[len(rd.CommittedEntries)-1].Index
			if err := pr.startApplyCommittedEntriesJob(rd.CommittedEntries); err != nil {
				log.Fatalf("raftstore[db-%d]: add apply committed entries job failed, errors:\n %+v",
					pr.id,
					err)
			}
		}
	}
}

func (pr *PeerReplicate) propose(c *cmd) {
	if !pr.checkProposal(c) {
		return
	}

	isConfChange := false
	policy := pr.getHandlePolicy(c.req)

	doPropose := false
	switch policy {
	case proposeNormal:
		doPropose = pr.proposeNormal(c)
	case proposeTransferLeader:
		doPropose = pr.proposeTransferLeader(c)
	case proposeChange:
		isConfChange = true
		doPropose = pr.proposeConfChange(c)
	}

	if !doPropose {
		return
	}

	err := pr.startProposeJob(c, isConfChange)
	if err != nil {
		c.respError(errorOtherCMDResp(nil, err))
	}
}

func (pr *PeerReplicate) proposeNormal(c *cmd) bool {
	if !pr.isLeader() {
		c.respError(errorNotLeader(nil, pr.id, pr.store.getPeer(pr.leaderPeerID())))
		return false
	}

	data := pbutil.MustMarshal(c.req)
	size := uint64(len(data))
	if size > pr.store.cfg.MaxRaftEntryBytes {
		c.respError(errorLargeRaftEntrySize(nil, pr.id, size))
		return false
	}

	idx := pr.nextProposalIndex()
	err := pr.rn.Propose(data)
	if err != nil {
		c.resp(errorOtherCMDResp(nil, err))
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		c.respError(errorNotLeader(nil, pr.id, pr.store.getPeer(pr.leaderPeerID())))
		return false
	}

	return true
}

func (pr *PeerReplicate) proposeConfChange(c *cmd) bool {
	err := pr.checkConfChange(c)
	if err != nil {
		c.respError(errorOtherCMDResp(nil, err))
		return false
	}

	cc := new(etcdraftpb.ConfChange)
	cc.Type = c.req.Admin.ChangePeer.ChangeType
	cc.NodeID = c.req.Admin.ChangePeer.Peer.ID
	cc.Context = pbutil.MustMarshal(c.req)

	idx := pr.nextProposalIndex()
	err = pr.rn.ProposeConfChange(*cc)
	if err != nil {
		c.respError(errorOtherCMDResp(nil, err))
		return false
	}
	idx2 := pr.nextProposalIndex()
	if idx == idx2 {
		c.respError(errorNotLeader(nil, pr.id, pr.store.getPeer(pr.leaderPeerID())))
		return false
	}

	log.Infof("raftstore[db-%d]: propose conf change, type=<%s> peer=<%d>",
		pr.id,
		cc.Type.String(),
		cc.NodeID)

	return true
}

/// Check whether it's safe to propose the specified conf change request.
/// It's safe iff at least the quorum of the Raft group is still healthy
/// right after that conf change is applied.
/// Define the total number of nodes in current Raft cluster to be `total`.
/// To ensure the above safety, if the cmd is
/// 1. A `AddNode` request
///    Then at least '(total + 1)/2 + 1' nodes need to be up to date for now.
/// 2. A `RemoveNode` request
///    Then at least '(total - 1)/2 + 1' other nodes (the node about to be removed is excluded)
///    need to be up to date for now.
func (pr *PeerReplicate) checkConfChange(c *cmd) error {
	changePeer := c.req.Admin.ChangePeer
	total := len(pr.rn.Status().Progress)
	if total == 1 {
		// It's always safe if there is only one node in the cluster.
		return nil
	}

	peer := changePeer.Peer
	switch changePeer.ChangeType {
	case etcdraftpb.ConfChangeAddNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			total++
		}
	case etcdraftpb.ConfChangeRemoveNode:
		if _, ok := pr.rn.Status().Progress[peer.ID]; !ok {
			return nil
		}
		total--
	}

	healthy := pr.countHealthyNode()
	quorumAfterChange := total/2 + 1

	if healthy >= quorumAfterChange {
		return nil
	}

	log.Infof("raftstore[db-%d]: rejects unsafe conf change request, total=<%d> healthy=<%d> quorum after change=<%d>",
		pr.id,
		total,
		healthy,
		quorumAfterChange)

	return fmt.Errorf("unsafe to perform conf change, total=<%d> healthy=<%d> quorum after change=<%d>",
		total,
		healthy,
		quorumAfterChange)
}

/// Count the number of the healthy nodes.
/// A node is healthy when
/// 1. it's the leader of the Raft group, which has the latest logs
/// 2. it's a follower, and it does not lag behind the leader a lot.
///    If a snapshot is involved between it and the Raft leader, it's not healthy since
///    it cannot works as a node in the quorum to receive replicating logs from leader.
func (pr *PeerReplicate) countHealthyNode() int {
	healthy := 0
	for _, p := range pr.rn.Status().Progress {
		if p.Match >= pr.ps.truncatedIndex() {
			healthy++
		}
	}

	return healthy
}

func (pr *PeerReplicate) proposeTransferLeader(c *cmd) bool {
	req := c.req.Admin

	if pr.isTransferLeaderAllowed(req.TransferLeader.Peer) {
		pr.doTransferLeader(req.TransferLeader.Peer)
	} else {
		log.Infof("raftstore[db-%d]: transfer leader ignored directly, req=<%+v>",
			pr.id,
			req)
	}

	// transfer leader command doesn't need to replicate log and apply, so we
	// return immediately. Note that this command may fail, we can view it just as an advice
	return false
}

func (pr *PeerReplicate) doTransferLeader(peer meta.Peer) {
	log.Infof("raftstore[db-%d]: transfer leader to %d",
		pr.id,
		peer.ID)

	pr.rn.TransferLeader(peer.ID)
}

func (pr *PeerReplicate) isTransferLeaderAllowed(newLeaderPeer meta.Peer) bool {
	status := pr.rn.Status()

	if _, ok := status.Progress[newLeaderPeer.ID]; !ok {
		return false
	}

	for _, p := range status.Progress {
		if p.State == etcdraft.ProgressStateSnapshot {
			return false
		}
	}

	lastIndex, _ := pr.ps.LastIndex()
	return lastIndex <= status.Progress[newLeaderPeer.ID].Match+maxTransferLeaderAllowLogLag
}

func (pr *PeerReplicate) checkProposal(c *cmd) bool {
	// we handle all read, write and admin cmd here
	if len(c.req.Header.UUID) == 0 {
		c.respError(errorOtherCMDResp(nil, errMissingUUIDCMD))
		return false
	}

	err := pr.store.validateStoreID(c.req)
	if err != nil {
		c.respError(errorOtherCMDResp(nil, err))
		return false
	}

	term := pr.getCurrentTerm()
	pe := pr.store.validateDB(c.req)
	if err != nil {
		c.respError(pe)
		return false
	}

	// Note:
	// The peer that is being checked is a leader. It might step down to be a follower later. It
	// doesn't matter whether the peer is a leader or not. If it's not a leader, the proposing
	// command log entry can't be committed.
	c.term = term
	return true
}

func (pr *PeerReplicate) getHandlePolicy(req *raftpb.RaftCMDRequest) requestPolicy {
	if req.Admin != nil {
		switch req.Admin.Type {
		case raftpb.ChangePeer:
			return proposeChange
		case raftpb.TransferLeader:
			return proposeTransferLeader
		default:
			return proposeNormal
		}
	}

	return proposeNormal
}

func (ps *peerStorage) doAppendSnap(ctx *readyContext, snap etcdraftpb.Snapshot) error {
	log.Infof("raftstore[db-%d]: begin to apply snapshot", ps.db.ID)

	if ctx.snap.Header.DB.ID != ps.db.ID {
		return fmt.Errorf("raftstore[db-%d]: db not match, snap is %d, current is %d",
			ps.db.ID,
			ctx.snap.Header.DB.ID,
			ps.db.ID)
	}

	if ps.isInitialized() {
		err := ps.store.clearMeta(ps.db.ID, ctx.wb)
		if err != nil {
			log.Errorf("raftstore[db-%d]: clear meta failed, errors:\n %+v",
				ps.db.ID,
				err)
			return err
		}
	}

	err := ps.updatePeerState(ctx.snap.Header.DB, raftpb.Applying, ctx.wb)
	if err != nil {
		log.Errorf("raftstore[db-%d]: write peer state failed, errors:\n %+v",
			ps.db.ID,
			err)
		return err
	}

	lastIndex := snap.Metadata.Index
	lastTerm := snap.Metadata.Term

	ctx.raftState.LastIndex = lastIndex
	ctx.applyState.AppliedIndex = lastIndex
	ctx.applyState.CommittedOffset = ctx.snap.Header.CommittedOffset
	ctx.applyState.CommittedIndex = ctx.snap.Header.CommittedIndex
	ctx.lastTerm = lastTerm

	// The snapshot only contains log which index > applied index, so
	// here the truncate state's (index, term) is in snapshot metadata.
	ctx.applyState.TruncatedState.Index = lastIndex
	ctx.applyState.TruncatedState.Term = lastTerm

	log.Infof("raftstore[db-%d]: apply snapshot ok, state %v",
		ps.db.ID,
		ctx.applyState)

	return nil
}

// doAppendEntries the given entries to the raft log using previous last index or self.last_index.
// Return the new last index for later update. After we commit in engine, we can set last_index
// to the return one.
func (ps *peerStorage) doAppendEntries(ctx *readyContext, entries []etcdraftpb.Entry) error {
	c := len(entries)
	if c == 0 {
		return nil
	}

	log.Debugf("raftstore[db-%d]: append %d entries",
		ps.db.ID,
		c)

	prevLastIndex := ctx.raftState.LastIndex
	lastIndex := entries[c-1].Index
	lastTerm := entries[c-1].Term

	for _, e := range entries {
		value := pbutil.MustMarshal(&e)
		err := ctx.wb.Set(getRaftLogKey(ps.db.ID, e.Index), value)
		if err != nil {
			log.Fatalf("raftstore[db-%d]: append entry failure, entry %v, errors:\n %+v",
				ps.db.ID,
				e,
				err)
			return err
		}
	}

	// Delete any previously appended log entries which never committed.
	for index := lastIndex + 1; index < prevLastIndex+1; index++ {
		err := ctx.wb.Delete(getRaftLogKey(ps.db.ID, index))
		if err != nil {
			log.Fatalf("raftstore[db-%d]: delete any previously appended log entries failure using %d, errors:\n %+v",
				ps.db.ID,
				index,
				err)
			return err
		}
	}

	ctx.raftState.LastIndex = lastIndex
	ctx.lastTerm = lastTerm

	return nil
}

func (pr *PeerReplicate) doSaveRaftState(ctx *readyContext) {
	readyState := ctx.raftState
	origin := pr.ps.raftlocalState

	if readyState.LastIndex != origin.LastIndex ||
		readyState.HardState.Commit != origin.HardState.Commit ||
		readyState.HardState.Term != origin.HardState.Term ||
		readyState.HardState.Vote != origin.HardState.Vote {
		err := ctx.wb.Set(getRaftLocalStateKey(pr.id), pbutil.MustMarshal(&readyState))
		if err != nil {
			log.Fatalf("raftstore[db-%d]: handle raft ready failure, errors:\n %+v",
				pr.id,
				err)
		}
	}
}

func (pr *PeerReplicate) doSaveApplyState(ctx *readyContext) {
	readyState := ctx.applyState
	origin := pr.ps.raftApplyState

	if readyState.AppliedIndex != origin.AppliedIndex ||
		readyState.TruncatedState.Index != origin.TruncatedState.Index ||
		readyState.TruncatedState.Term != origin.TruncatedState.Term ||
		readyState.CommittedOffset != origin.CommittedOffset {
		err := ctx.wb.Set(getRaftApplyStateKey(pr.id), pbutil.MustMarshal(&readyState))
		if err != nil {
			log.Fatalf("raftstore[db-%d]: handle raft ready failure, errors:\n %+v",
				pr.id,
				err)
		}
	}
}

func (pr *PeerReplicate) doApplySnap(ctx *readyContext, rd etcdraft.Ready) *applySnapResult {
	pr.ps.raftlocalState = ctx.raftState
	pr.ps.raftApplyState = ctx.applyState
	pr.ps.lastTerm = ctx.lastTerm

	// If we apply snapshot ok, we should update some infos like applied index too.
	if ctx.snap == nil {
		return nil
	}

	pr.startApplySnapJob()

	prevDB := pr.ps.db
	pr.ps.db = ctx.snap.Header.DB
	return &applySnapResult{
		prev: prevDB,
		db:   pr.ps.db,
	}
}

func (pr *PeerReplicate) handleCheckCompact() {
	// Leader will replicate the compact log command to followers,
	// If we use current replicated_index (like 10) as the compact index,
	// when we replicate this log, the newest replicated_index will be 11,
	// but we only compact the log to 10, not 11, at that time,
	// the first index is 10, and replicated_index is 11, with an extra log,
	// and we will do compact again with compact index 11, in cycles...
	// So we introduce a threshold, if replicated index - first index > threshold,
	// we will try to compact log.
	// raft log entries[..............................................]
	//                  ^                                       ^
	//                  |-----------------threshold------------ |
	//              first_index                         replicated_index

	var replicatedIdx uint64
	for _, p := range pr.rn.Status().Progress {
		if replicatedIdx == 0 {
			replicatedIdx = p.Match
		}

		if p.Match < replicatedIdx {
			replicatedIdx = p.Match
		}
	}

	// When an election happened or a new peer is added, replicated_idx can be 0.
	if replicatedIdx > 0 {
		lastIdx := pr.rn.LastIndex()
		if lastIdx < replicatedIdx {
			log.Fatalf("raftstore[db-%d]: expect last index %d >= replicated index %d",
				pr.id,
				lastIdx,
				replicatedIdx)
		}
	}

	var compactIdx uint64
	appliedIdx := pr.ps.appliedIndex()
	firstIdx, _ := pr.ps.FirstIndex()

	if replicatedIdx < firstIdx ||
		replicatedIdx-firstIdx <= pr.store.cfg.MinRaftLogCount {
		return
	}

	if appliedIdx > firstIdx &&
		appliedIdx-firstIdx >= pr.store.cfg.MaxRaftLogCount {
		compactIdx = appliedIdx
	} else if pr.raftLogSizeHint >= pr.store.cfg.MaxRaftLogBytes {
		compactIdx = appliedIdx
	} else {
		compactIdx = replicatedIdx
	}

	// Have no idea why subtract 1 here, but original code did this by magic.
	if compactIdx == 0 {
		log.Fatal("raftstore[db-%d]: unexpect compactIdx",
			pr.id)
	}

	// avoid leader send snapshot to the a little lag peer.
	if compactIdx > replicatedIdx {
		if (compactIdx - replicatedIdx) <= pr.store.cfg.MaxRaftLogLag {
			compactIdx = replicatedIdx
		} else {
			log.Infof("raftstore[db-%d]: peer lag %d is too large, maybe sent a snapshot later",
				pr.id,
				compactIdx-replicatedIdx)
		}
	}

	compactIdx--
	if compactIdx < firstIdx {
		// In case compactIdx == firstIdx before subtraction.
		return
	}

	term, _ := pr.rn.Term(compactIdx)
	pr.onAdmin(&raftpb.AdminRequest{
		Type: raftpb.CompactLog,
		CompactLog: &raftpb.CompactLogRequest{
			CompactIndex: compactIdx,
			CompactTerm:  term,
		},
	})
}

func (pr *PeerReplicate) doPollApply(result *asyncApplyResult) {
	pr.doPostApply(result)
	if result.result != nil {
		pr.store.doPostApplyResult(result)
	}
}

func (pr *PeerReplicate) doPostApply(result *asyncApplyResult) {
	if pr.ps.isApplyingSnap() {
		log.Fatalf("raftstore[db-%d]: should not applying snapshot, when do post apply.",
			pr.id)
	}

	pr.ps.setRaftApplyState(result.applyState)
	pr.ps.vectorRecords += result.writtenRecords
	pr.ps.appliedIndexTerm = result.appliedIndexTerm
	pr.rn.AdvanceApply(result.applyState.AppliedIndex)

	log.Debugf("raftstore[db-%d]: async apply committied entries finished, applied=<%d>",
		pr.id,
		result.applyState.AppliedIndex)
}

func (pr *PeerReplicate) maybeSplit() {
	if pr.ps.availableWriteRecords() <= 0 {
		pr.maybeStopConsumer()

		if pr.isLeader() && !pr.alreadySplit && pr.isWritable() {
			pr.alreadySplit = true
			offset, index := pr.ps.committedOffset()

			log.Infof("raftstore[db-%d]: %d reach the maximum limit %d per db, split at committed offset %d, index %d, %+v",
				pr.id,
				pr.ps.vectorRecords,
				pr.store.cfg.MaxDBRecords,
				offset,
				index,
				pr.ps.db)

			// notify wait search, maybe choose next range db
			pr.cond.Broadcast()

			pr.startAskSplitJob(pr.ps.db, offset, index)
		}
	}
}

func (s *Store) doPostApplyResult(result *asyncApplyResult) {
	switch result.result.adminType {
	case raftpb.ChangePeer:
		s.doApplyConfChange(result.id, result.result.changePeer)
	case raftpb.Split:
		s.doApplySplit(result.id, result.result.splitResult)
	case raftpb.CompactLog:
		s.doApplyCompactRaftLog(result.id, result.result.compactResult)
	}
}

func (s *Store) doApplyConfChange(id uint64, cp *changePeer) {
	pr := s.getDB(id, false)
	if nil == pr {
		log.Fatalf("raftstore[db-%d]: missing cell",
			id)
	}

	pr.rn.ApplyConfChange(cp.confChange)
	if cp.confChange.NodeID == 0 {
		// Apply failed, skip.
		return
	}

	pr.ps.db = cp.db
	if pr.isLeader() {
		// Notify pd immediately.
		log.Infof("raftstore[db-%d]: notify prophet with change peer, db=<%+v>",
			id,
			cp.db)
		pr.store.pd.GetRPC().TiggerResourceHeartbeat(pr.id)
	}

	switch cp.confChange.Type {
	case etcdraftpb.ConfChangeAddNode:
		// Add this peer to cache.
		pr.heartbeatsMap.Store(cp.peer.ID, time.Now())
		s.addPeerToCache(cp.peer)
	case etcdraftpb.ConfChangeRemoveNode:
		// Remove this peer from cache.
		pr.heartbeatsMap.Delete(cp.peer.ID)
		s.peers.Delete(cp.peer.ID)

		// We only care remove itself now.
		if cp.peer.StoreID == pr.store.meta.ID {
			if cp.peer.ID == pr.peer.ID {
				// sync remove self peer, and stop the raft event loop
				s.destroyPeer(id, cp.peer, false)
				return
			}

			log.Fatalf("raftstore[db-%d]: trying to remove unknown peer, peer=<%+v>",
				id,
				cp.peer)
		}
	}

	if pr.isLeader() {
		pr.addRequest(acquireReqCtx())
	}
}

func (s *Store) doApplyCompactRaftLog(id uint64, result *compactLogResult) {
	pr := s.getDB(id, false)
	if pr != nil {
		total := pr.ps.lastReadyIndex - result.firstIndex
		remain := pr.ps.lastReadyIndex - result.state.Index - 1
		pr.raftLogSizeHint = pr.raftLogSizeHint * remain / total

		startIndex := pr.ps.lastCompactIndex
		endIndex := result.state.Index + 1
		pr.ps.lastCompactIndex = endIndex

		log.Debugf("raftstore[db-%d]: start to compact raft log, start=<%d> end=<%d>",
			id,
			startIndex,
			endIndex)
		err := pr.startCompactRaftLogJob(id, startIndex, endIndex)
		if err != nil {
			log.Errorf("raftstore[db-%d]: add compact raft log job failed, errors:\n %+v",
				id,
				err)
		}
	}
}

func (s *Store) doApplySplit(id uint64, result *splitResult) {
	pr := s.getDB(id, false)
	if nil == pr {
		log.Fatalf("raftstore[db-%d]: missing db",
			id)
	}

	pr.alreadySplit = true
	if !result.valid {
		pr.alreadySplit = false
		return
	}

	oldDB := result.oldDB
	newDB := result.newDB
	pr.ps.db = oldDB
	newDBID := newDB.ID
	newPR := s.getDB(newDBID, false)
	if nil != newPR {
		// If the store received a raft msg with the new db raft group
		// before splitting, it will creates a uninitialized peer.
		// We can remove this uninitialized peer directly.
		if newPR.ps.isInitialized() {
			log.Fatalf("raftstore[db-%d]: new db created by raft message before apply split, drop it and create again",
				newPR.id)
		}
	}

	for _, p := range newDB.Peers {
		s.addPeerToCache(*p)
	}

	newPR, err := createPeerReplicate(s, &newDB)
	if err != nil {
		// peer information is already written into db, can't recover.
		// there is probably a bug.
		log.Fatalf("raftstore[db-%d]: create new split db failed, newDB=<%d> errors:\n %+v",
			id,
			newDB,
			err)
	}

	newPR.startRegistrationJob()
	s.replicates.Store(newPR.id, newPR)

	// If this peer is the leader of the cell before split, it's intuitional for
	// it to become the leader of new split cell.
	// The ticks are accelerated here, so that the peer for the new split cell
	// comes to campaign earlier than the other follower peers. And then it's more
	// likely for this peer to become the leader of the new split cell.
	// If the other follower peers applies logs too slowly, they may fail to vote the
	// `MsgRequestVote` from this peer on its campaign.
	// In this worst case scenario, the new split raft group will not be available
	// since there is no leader established during one election timeout after the split.
	if pr.isLeader() && len(newDB.Peers) > 1 {
		newPR.addAction(doCampaignAction)
	}

	if pr.isLeader() {
		log.Infof("raftstore[db-%d]: notify pd with split, old=<%+v> new=<%+v>, state=<%s>, apply=<%s>",
			id,
			oldDB,
			newDB,
			pr.ps.raftlocalState.String(),
			pr.ps.raftApplyState.String())

		pr.store.pd.GetRPC().TiggerResourceHeartbeat(pr.id)
	} else {
		if vote, ok := pr.store.removeDroppedVoteMsg(newPR.id); ok {
			newPR.step(vote)
		}
	}

	log.Infof("raftstore[db-%d]: new db added, old=<%+v> new=<%+v>",
		id,
		oldDB,
		newDB)
}

func (pr *PeerReplicate) send(msgs []etcdraftpb.Message) {
	for _, msg := range msgs {
		err := pr.sendRaftMsg(msg)
		if err != nil {
			// We don't care that the message is sent failed, so here just log this error
			log.Warnf("raftstore[db-%d]: send msg failure, from_peer=<%d> to_peer=<%d>, errors:\n%s",
				pr.id,
				msg.From,
				msg.To,
				err)
		}
	}
}

func (pr *PeerReplicate) sendRaftMsg(msg etcdraftpb.Message) error {
	sendMsg := util.AcquireRaftMessage()
	sendMsg.ID = pr.id
	sendMsg.Epoch = pr.ps.db.Epoch
	sendMsg.Start = pr.ps.db.Start
	sendMsg.DBState = pr.ps.db.State

	sendMsg.From = pr.peer

	toPeer := pr.store.getPeer(msg.To)
	if toPeer == nil {
		return fmt.Errorf("can not found peer<%d>", msg.To)
	}
	sendMsg.To = *toPeer
	sendMsg.Message = msg
	pr.store.trans.sendRaftMessage(sendMsg)

	switch msg.Type {
	// case raftpb.MsgApp:
	// 	pr.metrics.message.append++
	// case raftpb.MsgAppResp:
	// 	pr.metrics.message.appendResp++
	// case raftpb.MsgVote:
	// 	pr.metrics.message.vote++
	// case raftpb.MsgVoteResp:
	// 	pr.metrics.message.voteResp++
	case etcdraftpb.MsgSnap:
		pr.rn.ReportSnapshot(msg.To, etcdraft.SnapshotFinish)
		// 	pr.metrics.message.snapshot++
		// case raftpb.MsgHeartbeat:
		// 	pr.metrics.message.heartbeat++
		// case raftpb.MsgHeartbeatResp:
		// 	pr.metrics.message.heartbeatResp++
		// case raftpb.MsgTransferLeader:
		// 	pr.metrics.message.transfeLeader++
	}

	return nil
}
