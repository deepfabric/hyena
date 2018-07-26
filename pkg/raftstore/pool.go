package raftstore

import (
	"sync"

	"github.com/fagongzi/goetty"

	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
)

var (
	cmdPool              sync.Pool
	readyContextPool     sync.Pool
	applyContextPool     sync.Pool
	asyncApplyResultPool sync.Pool
	reqCtxPool           sync.Pool
	vdbBatchPool         sync.Pool
	bufPool              sync.Pool

	emptyRaftState  = raftpb.RaftLocalState{}
	emptyApplyState = raftpb.RaftApplyState{}
)

func acquireVdbBatch() *vdbBatch {
	v := vdbBatchPool.Get()
	if v == nil {
		return &vdbBatch{}
	}

	return v.(*vdbBatch)
}

func releaseVdbBatch(value *vdbBatch) {
	value.reset()
	vdbBatchPool.Put(value)
}

func acquireReadyContext() *readyContext {
	v := readyContextPool.Get()
	if v == nil {
		return &readyContext{}
	}

	return v.(*readyContext)
}

func releaseReadyContext(ctx *readyContext) {
	ctx.reset()
	readyContextPool.Put(ctx)
}

func acquireApplyContext() *applyContext {
	v := applyContextPool.Get()
	if v == nil {
		return &applyContext{}
	}

	return v.(*applyContext)
}

func releaseApplyContext(ctx *applyContext) {
	ctx.reset()
	applyContextPool.Put(ctx)
}

func acquireAsyncApplyResult() *asyncApplyResult {
	v := asyncApplyResultPool.Get()
	if v == nil {
		return &asyncApplyResult{}
	}

	return v.(*asyncApplyResult)
}

func releaseAsyncApplyResult(res *asyncApplyResult) {
	res.reset()
	asyncApplyResultPool.Put(res)
}

func acquireReqCtx() *reqCtx {
	v := reqCtxPool.Get()
	if v == nil {
		return &reqCtx{}
	}

	return v.(*reqCtx)
}

func releaseReqCtx(value *reqCtx) {
	value.reset()
	reqCtxPool.Put(value)
}

func acquireCmd() *cmd {
	v := cmdPool.Get()
	if v == nil {
		return &cmd{}
	}

	return v.(*cmd)
}

func releaseCmd(value *cmd) {
	value.reset()
	cmdPool.Put(value)
}

func acquireBuf(size int) *goetty.ByteBuf {
	v := bufPool.Get()
	if v == nil {
		return goetty.NewByteBuf(size)
	}

	value := v.(*goetty.ByteBuf)
	value.Resume(size)
	return value
}

func releaseBuf(value *goetty.ByteBuf) {
	value.Clear()
	value.Release()
	bufPool.Put(value)
}
