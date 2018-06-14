package raftstore

import (
	"bytes"
	"encoding/hex"
	"sync"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/deepfabric/storage"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
)

type applySnapResult struct {
	prev meta.VectorDB
	db   meta.VectorDB
}

type asyncApplyResult struct {
	id               uint64
	writtenRecords   uint64
	appliedIndexTerm uint64
	applyState       raftpb.RaftApplyState
	result           *execResult
}

func (res *asyncApplyResult) reset() {
	res.id = 0
	res.appliedIndexTerm = 0
	res.writtenRecords = 0
	res.applyState = emptyApplyState
	res.result = nil
}

type execResult struct {
	adminType     raftpb.AdminCmdType
	changePeer    *changePeer
	splitResult   *splitResult
	compactResult *compactLogResult
}

type changePeer struct {
	confChange etcdraftpb.ConfChange
	peer       meta.Peer
	db         meta.VectorDB
}

type splitResult struct {
	oldDB meta.VectorDB
	newDB meta.VectorDB
}

type compactLogResult struct {
	state      raftpb.RaftTruncatedState
	firstIndex uint64
}

type readyContext struct {
	raftState  raftpb.RaftLocalState
	applyState raftpb.RaftApplyState
	lastTerm   uint64
	snap       *raftpb.SnapshotMessage
	wb         storage.WriteBatch
}

func (ctx *readyContext) reset() {
	ctx.raftState = emptyRaftState
	ctx.applyState = emptyApplyState
	ctx.lastTerm = 0
	ctx.snap = nil
	ctx.wb = nil
}

type applyContext struct {
	index      uint64
	term       uint64
	applyState raftpb.RaftApplyState
	req        *raftpb.RaftCMDRequest
	resps      []interface{}

	vdbBatch *vdbBatch
	wb       storage.WriteBatch
}

func (ctx *applyContext) reset() {
	ctx.wb = nil
	ctx.applyState = emptyApplyState
	ctx.req = nil
	ctx.index = 0
	ctx.term = 0
}

type applyDelegate struct {
	sync.RWMutex

	peerID uint64
	store  *Store
	ps     *peerStorage
	db     meta.VectorDB

	// if we remove ourself in ChangePeer remove, we should set this flag, then
	// any following committed logs in same Ready should be applied failed.
	pendingRemove bool

	applyState       raftpb.RaftApplyState
	appliedIndexTerm uint64
	term             uint64

	pendingCMDs          []*cmd
	pendingChangePeerCMD *cmd
}

func (d *applyDelegate) clearAllCommandsAsStale() {
	d.Lock()
	for _, c := range d.pendingCMDs {
		d.notifyStaleCMD(c)
	}

	if nil != d.pendingChangePeerCMD {
		d.notifyStaleCMD(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = make([]*cmd, 0)
	d.pendingChangePeerCMD = nil
	d.Unlock()
}

func (d *applyDelegate) setPendingRemove() {
	d.Lock()
	d.pendingRemove = true
	d.Unlock()
}

func (d *applyDelegate) isPendingRemove() bool {
	d.RLock()
	value := d.pendingRemove
	d.RUnlock()

	return value
}

func (d *applyDelegate) destroy() {
	d.Lock()
	for _, c := range d.pendingCMDs {
		d.notifyDBRemoved(c)
	}

	if d.pendingChangePeerCMD != nil && d.pendingChangePeerCMD.req != nil {
		d.notifyDBRemoved(d.pendingChangePeerCMD)
	}

	d.pendingCMDs = nil
	d.pendingChangePeerCMD = nil
	d.Unlock()
}

func (d *applyDelegate) applyCommittedEntries(entries []etcdraftpb.Entry) {
	if len(entries) <= 0 {
		return
	}

	ctx := acquireApplyContext()
	req := util.AcquireRaftCMDRequest()

	for idx, entry := range entries {
		if d.isPendingRemove() {
			// This peer is about to be destroyed, skip everything.
			break
		}
		expectIndex := d.applyState.AppliedIndex + 1
		if expectIndex != entry.Index {
			log.Fatalf("raftstore[db-%d]: index not match, expect is %d, but %d",
				d.db.ID,
				expectIndex,
				entry.Index)
		}

		if idx > 0 {
			ctx.reset()
			req.Reset()
		}

		ctx.req = req
		ctx.applyState = d.applyState
		ctx.index = entry.Index
		ctx.term = entry.Term

		var result *execResult
		switch entry.Type {
		case etcdraftpb.EntryNormal:
			result = d.applyEntry(ctx, entry)
		case etcdraftpb.EntryConfChange:
			result = d.applyConfChange(ctx, entry)
		}

		asyncResult := acquireAsyncApplyResult()
		asyncResult.id = d.db.ID
		asyncResult.appliedIndexTerm = d.appliedIndexTerm
		asyncResult.applyState = d.applyState
		asyncResult.result = result

		if pr, ok := d.store.replicates.Load(d.db.ID); ok {
			pr.(*PeerReplicate).addApplyResult(asyncResult)
		}
	}

	// only release raft cmd request
	util.ReleaseRaftCMDRequest(req)
	releaseApplyContext(ctx)
}

func (d *applyDelegate) applyConfChange(ctx *applyContext, entry etcdraftpb.Entry) *execResult {
	cc := new(etcdraftpb.ConfChange)

	pbutil.MustUnmarshal(cc, entry.Data)
	pbutil.MustUnmarshal(ctx.req, cc.Context)

	result := d.doApplyRaftCMD(ctx)
	if nil == result {
		return &execResult{
			adminType:  raftpb.ChangePeer,
			changePeer: &changePeer{},
		}
	}

	result.changePeer.confChange = *cc
	return result
}

func (d *applyDelegate) applyEntry(ctx *applyContext, entry etcdraftpb.Entry) *execResult {
	if len(entry.Data) > 0 {
		pbutil.MustUnmarshal(ctx.req, entry.Data)
		return d.doApplyRaftCMD(ctx)
	}

	// when a peer become leader, it will send an empty entry for commit a pre term uncommitted logs.
	state := d.applyState
	state.AppliedIndex = entry.Index

	err := d.store.metaStore.Set(getRaftApplyStateKey(d.db.ID), pbutil.MustMarshal(&state), d.store.cfg.SyncWrite)
	if err != nil {
		log.Fatalf("raftstore-apply[db-%d]: apply empty entry failed, errors:\n %+v",
			d.db.ID,
			err)
	}

	d.applyState.AppliedIndex = entry.Index
	d.appliedIndexTerm = entry.Term
	if entry.Term <= 0 {
		log.Fatalf("raftstore-apply[db-%d]: error empty entry term",
			d.db.ID)
	}

	for {
		c := d.popPendingCMD(entry.Term - 1)
		if c == nil {
			return nil
		}

		// apprently, all the callbacks whose term is less than entry's term are stale.
		d.notifyStaleCMD(c)
	}
}

func (d *applyDelegate) doApplyRaftCMD(ctx *applyContext) *execResult {
	if ctx.index == 0 {
		log.Fatalf("raftstore[db-%d]: apply raft command needs a none zero index",
			d.db.ID)
	}

	c := d.findCB(ctx)
	if d.isPendingRemove() {
		log.Fatalf("raftstore[db-%d]: apply raft comand can not pending remove",
			d.db.ID)
	}

	var err error
	var errRsp *raftpb.Error
	var result *execResult

	ctx.wb = d.store.metaStore.NewWriteBatch()
	ctx.vdbBatch = acquireVdbBatch()

	if !checkEpoch(d.db, ctx.req) {
		errRsp = errorStaleEpochResp(d.db)
	} else {
		if ctx.req.Admin != nil {
			result, err = d.execAdminRequest(ctx)
			if err != nil {
				errRsp = errorStaleEpochResp(d.db)
			}
		} else {
			d.execWriteRequest(ctx)
		}
	}

	if ctx.vdbBatch != nil {

	}

	err = ctx.vdbBatch.do(d)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: save apply context failed, errors:\n %+v",
			d.db.ID,
			err)
	}
	releaseVdbBatch(ctx.vdbBatch)
	ctx.vdbBatch = nil

	ctx.applyState.AppliedIndex = ctx.index
	if !d.isPendingRemove() {
		err := ctx.wb.Set(getRaftApplyStateKey(d.db.ID), pbutil.MustMarshal(&ctx.applyState))
		if err != nil {
			log.Fatalf("raftstore[db-%d]: save apply context failed, errors:\n %+v",
				d.db.ID,
				err)
		}
	}

	err = d.store.metaStore.Write(ctx.wb, false)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: commit apply result failed, errors:\n %+v",
			d.db.ID,
			err)
	}

	d.applyState = ctx.applyState
	d.term = ctx.term

	if log.DebugEnabled() {
		log.Debugf("raftstore[db-%d]: applied command, index=<%d> uuid=<%s> state=<%+v>",
			d.db.ID,
			ctx.index,
			hex.EncodeToString(ctx.req.Header.UUID),
			d.applyState)
	}

	if c != nil {
		if errRsp != nil {
			c.respError(errRsp)
		} else {
			c.resp(ctx.resps)
		}
	}

	return result
}

func (d *applyDelegate) findCB(ctx *applyContext) *cmd {
	if isChangePeerCMD(ctx.req) {
		c := d.getPendingChangePeerCMD()
		if c == nil || c.req == nil {
			return nil
		} else if bytes.Compare(ctx.req.Header.UUID, c.req.Header.UUID) == 0 {
			return c
		}

		d.notifyStaleCMD(c)
		return nil
	}

	for {
		head := d.popPendingCMD(ctx.term)
		if head == nil {
			return nil
		}

		if bytes.Compare(head.req.Header.UUID, ctx.req.Header.UUID) == 0 {
			return head
		}

		if log.DebugEnabled() {
			log.Debugf("raftstore[db-%d]: notify stale cmd, cmd=<%v>",
				d.db.ID,
				head)
		}

		// Because of the lack of original RaftCmdRequest, we skip calling
		// coprocessor here.
		d.notifyStaleCMD(head)
	}
}

func (d *applyDelegate) popPendingCMD(term uint64) *cmd {
	d.Lock()
	if len(d.pendingCMDs) == 0 {
		d.Unlock()
		return nil
	}

	if d.pendingCMDs[0].term > term {
		d.Unlock()
		return nil
	}

	c := d.pendingCMDs[0]
	d.pendingCMDs[0] = nil
	d.pendingCMDs = d.pendingCMDs[1:]
	d.Unlock()
	return c
}

func (d *applyDelegate) notifyStaleCMD(c *cmd) {
	log.Debugf("raftstore[db-%d]: resp stale, cmd %v, term %d",
		d.db.ID,
		c,
		d.term)
	c.respError(errorStaleCMDResp())
}

func (d *applyDelegate) notifyDBRemoved(c *cmd) {
	log.Infof("raftstore[db-%d]: db is removed, skip cmd. cmd=<%+v>",
		d.db.ID,
		c)
	c.respDBNotFound(d.db.ID)
}

func (d *applyDelegate) appendPendingCmd(c *cmd) {
	d.pendingCMDs = append(d.pendingCMDs, c)
}

func (d *applyDelegate) getPendingChangePeerCMD() *cmd {
	d.RLock()
	c := d.pendingChangePeerCMD
	d.RUnlock()

	return c
}

func (d *applyDelegate) setPendingChangePeerCMD(c *cmd) {
	d.Lock()
	d.pendingChangePeerCMD = c
	d.Unlock()
}

func isChangePeerCMD(req *raftpb.RaftCMDRequest) bool {
	return nil != req.Admin &&
		req.Admin.Type == raftpb.ChangePeer
}
