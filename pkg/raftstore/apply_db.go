package raftstore

import (
	"context"

	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

const (
	updateBatch = 512
)

type vdbBatch struct {
	available uint64
	xbs       []float32
	ids       []int64
	doF       func(d *applyDelegate) error
}

func (b *vdbBatch) do(d *applyDelegate) error {
	if b.doF != nil {
		return b.doF(d)
	}

	return nil
}

func (b *vdbBatch) doInsert(d *applyDelegate) error {
	return d.ps.vdb.AddWithIds(b.xbs, b.ids)
}

func (b *vdbBatch) doUpdate(d *applyDelegate) error {
	return d.ps.vdb.UpdateWithIds(b.xbs, b.ids)
}

func (b *vdbBatch) append(xbs []float32, ids []int64) int64 {
	n := uint64(len(ids))
	if b.available >= n {
		b.available -= n
		b.xbs = append(b.xbs, xbs...)
		b.ids = append(b.ids, ids...)
		return indexComplete
	}

	d := uint64(len(xbs) / len(ids))
	b.ids = append(b.ids, ids[:b.available]...)
	b.xbs = append(b.xbs, xbs[:b.available*d]...)
	next := int64(b.available)
	b.available = 0
	return next - 1
}

func (b *vdbBatch) appendFromIndex(xbs []float32, ids []int64, index int64) int64 {
	n := int64(len(ids))
	d := int64(len(xbs) / len(ids))

	if n <= index+1 {
		log.Fatalf("bug: invalid mq consumer index: %d, only %d", index, n)
	}

	retIndex := b.append(xbs[(index+1)*d:], ids[index+1:])
	if retIndex == -1 {
		return -1
	}

	return index + retIndex + 1
}

func (b *vdbBatch) appendable() bool {
	return b.available > 0
}

func (b *vdbBatch) reset() {
	b.available = 0
	b.doF = nil
	b.xbs = nil
	b.ids = nil
}

func (d *applyDelegate) execWriteRequest(ctx *applyContext) {
	if len(ctx.req.Inserts) > 0 && len(ctx.req.Updates) > 0 {
		log.Fatalf("raftstore[db-%d]: bug insert and update can't in a batch", d.ps.db.ID)
	}

	if len(ctx.req.Inserts) > 0 {
		ctx.vdbBatch.doF = ctx.vdbBatch.doInsert
	} else if len(ctx.req.Updates) > 0 {
		ctx.vdbBatch.doF = ctx.vdbBatch.doUpdate
	}

	for _, req := range ctx.req.Inserts {
		d.execInsert(ctx, req)
	}

	for _, req := range ctx.req.Updates {
		d.execUpdate(ctx, req)
	}
}

func (d *applyDelegate) execInsert(ctx *applyContext, req *rpc.InsertRequest) {
	ctx.vdbBatch.append(req.Xbs, req.Ids)
	rsp := util.AcquireInsertRsp()
	rsp.ID = req.ID
	ctx.resps = append(ctx.resps, rsp)
}

func (d *applyDelegate) execUpdate(ctx *applyContext, req *rpc.UpdateRequest) {
	ctx.vdbBatch.append(req.Xbs, req.Ids)
	rsp := util.AcquireUpdateRsp()
	rsp.ID = req.ID
	ctx.resps = append(ctx.resps, rsp)
}

func (pr *PeerReplicate) execSearch(req *rpc.SearchRequest, cb func(interface{}), searchNext bool) {
	n := len(req.Xq) / pr.store.cfg.Dim
	ds := make([]float32, n, n)
	ids := make([]int64, n, n)

	_, err := pr.ps.vdb.Search(req.Xq, ds, ids)
	if err != nil {
		cb(errorOtherCMDResp(req.ID, err))
		return
	}

	rsp := util.AcquireSearchRsp()
	rsp.ID = req.ID
	rsp.Distances = ds
	rsp.Xids = ids
	rsp.DB = pr.id
	rsp.SearchNext = searchNext
	cb(rsp)
}

func (pr *PeerReplicate) asyncExecUpdates(ctx context.Context) {
	items := make([]interface{}, updateBatch, updateBatch)

	for {
		select {
		case <-ctx.Done():
			log.Infof("raftstore[db-%d]: handle update requests stopped",
				pr.id)
			return
		default:
			n, err := pr.mqUpdateRequests.Get(updateBatch, items)
			if err != nil {
				continue
			}

			batchReq := util.AcquireUpdateReq()
			for i := int64(0); i < n; i++ {
				req := items[i].(*rpc.UpdateRequest)
				batchReq.Xbs = append(batchReq.Xbs, req.Xbs...)
				batchReq.Ids = append(batchReq.Ids, req.Ids...)
				util.ReleaseUpdateReq(req)
			}

			err = pr.ps.vdb.UpdateWithIds(batchReq.Xbs, batchReq.Ids)
			if err != nil {
				log.Errorf("raftstore[db-%d]: exec update failed: %+v",
					pr.id,
					err)
				return
			}
			util.ReleaseUpdateReq(batchReq)
		}
	}
}
