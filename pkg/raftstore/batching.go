package raftstore

import (
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

func (pr *PeerReplicate) resetBatching() {
	pr.batching = newBatching(pr)
}

type batching struct {
	pr *PeerReplicate

	maxPerBatch int
	lastType    int32
	batches     []*cmd
}

func newBatching(pr *PeerReplicate) *batching {
	return &batching{
		pr:          pr,
		maxPerBatch: pr.store.cfg.MaxBatchingSize,
	}
}

func (b *batching) pop() *cmd {
	if b.isEmpty() {
		return nil
	}

	value := b.batches[0]
	b.batches[0] = nil
	b.batches = b.batches[1:]

	return value
}

func (b *batching) put(req *reqCtx) {
	last := b.lastBatch()

	if last == nil ||
		req.msgType == int32(rpc.MsgAdmin) || // admin request must in a single batch
		b.lastType != req.msgType ||
		b.isFull(last) {
		b.batches = append(b.batches, newCMD(req, b.pr))
	} else {
		last.append(req)
	}

	b.lastType = req.msgType
	releaseReqCtx(req)
}

func (b *batching) lastBatch() *cmd {
	if b.isEmpty() {
		return nil
	}

	return b.batches[b.size()-1]
}

func (b *batching) isEmpty() bool {
	return 0 == b.size()
}

func (b *batching) isFull(c *cmd) bool {
	return b.maxPerBatch == len(c.req.Inserts) ||
		b.maxPerBatch == len(c.req.Updates)
}

func (b *batching) size() int {
	return len(b.batches)
}
