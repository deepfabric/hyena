package raftstore

import (
	"github.com/fagongzi/log"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

type vdbBatch struct {
	xbs []float32
	ids []int64
	doF func(d *applyDelegate) error
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

func (b *vdbBatch) append(xbs []float32, ids []int64) {
	b.xbs = append(b.xbs, xbs...)
	b.ids = append(b.ids, ids...)
}

func (b *vdbBatch) reset() {
	b.doF = nil
	b.xbs = b.xbs[:0]
	b.ids = b.ids[:0]
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

func (pr *PeerReplicate) execSearch(req *rpc.SearchRequest, cb func(interface{}), cbErr func([]byte, *raftpb.Error)) {
	n := len(req.Xq)
	ds := make([]float32, n, n)
	ids := make([]int64, n, n)

	_, err := pr.ps.vdb.Search(req.Xq, ds, ids)
	if err != nil {
		cbErr(req.ID, errorOtherCMDResp(err))
		return
	}
}
