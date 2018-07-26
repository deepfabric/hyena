package proxy

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/uuid"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

type asyncContext struct {
	sync.Mutex
	timeouts            sync.Map
	completeC, timeoutC chan struct{}
	to, received        uint64
	distances           []float32
	ids                 []int64
}

func (ctx *asyncContext) addTimeout(id interface{}, timeout *goetty.Timeout) {
	ctx.timeouts.Store(id, timeout)
}

func (ctx *asyncContext) done(rsp *rpc.SearchResponse) {
	if value, ok := ctx.timeouts.Load(rsp.ID); ok {
		value.(*goetty.Timeout).Stop()
		ctx.timeouts.Delete(rsp.ID)
		ctx.Lock()
		for idx, value := range rsp.Xids {
			if value != -1 && betterThan(rsp.Distances[idx], ctx.distances[idx]) {
				ctx.distances[idx] = rsp.Distances[idx]
				ctx.ids[idx] = value
			}
		}
		ctx.Unlock()
	}

	newV := atomic.AddUint64(&ctx.received, 1)
	if newV == ctx.to {
		ctx.completeC <- struct{}{}
	}
}

func (ctx *asyncContext) onTimeout(arg interface{}) {
	ctx.timeoutC <- struct{}{}
}

func (ctx *asyncContext) get(timeout time.Duration) ([]float32, []int64, error) {
	util.DefaultTimeoutWheel().Schedule(timeout, ctx.onTimeout, nil)

	select {
	case <-ctx.completeC:
		return ctx.distances, ctx.ids, nil
	case <-ctx.timeoutC:
		log.Warnf("search timeout")
		return ctx.distances, ctx.ids, fmt.Errorf("timeout")
	}
}

func (ctx *asyncContext) reset() {
	// reset counter
	ctx.to = 0
	ctx.received = 0

	// clear map
	ctx.timeouts.Range(func(key, value interface{}) bool {
		value.(*goetty.Timeout).Stop()
		ctx.timeouts.Delete(key)
		return true
	})

	// clear chan
	for {
		select {
		case <-ctx.completeC:
		case <-ctx.timeoutC:
		default:
			close(ctx.completeC)
			close(ctx.timeoutC)
			return
		}
	}
}

func (r *router) onTimeout(arg interface{}) {
	r.ctxs.Delete(arg)
}

func (r *router) addAsyncCtx(ctx *asyncContext, req *rpc.SearchRequest) {
	r.ctxs.Store(req.ID, ctx)
	timeout, err := util.DefaultTimeoutWheel().Schedule(r.timeout, r.onTimeout, req.ID)
	if err != nil {
		log.Fatal("bug: timeout can not added failed")
	}
	ctx.addTimeout(req.ID, &timeout)
}

func (r *router) search(req *rpc.SearchRequest) ([]float32, []int64, error) {
	ctx := acquireCtx()

	r.RLock()
	l := len(req.Xq)
	ctx.to = uint64(len(r.dbs))
	ctx.timeoutC = make(chan struct{}, ctx.to)
	ctx.distances = make([]float32, l, l)
	ctx.ids = make([]int64, l, l)
	for id, db := range r.dbs {
		bReq := &rpc.SearchRequest{
			ID: uuid.NewV4().Bytes(),
			DB: id,
			Xq: req.Xq,
		}
		r.addAsyncCtx(ctx, bReq)
		r.transports[r.selectTargetPeer(db).StoreID].sent(bReq)
	}
	r.RUnlock()

	ds, ids, err := ctx.get(r.timeout)
	releaseCtx(ctx)
	return ds, ids, err
}

func (r *router) onResponse(msg interface{}) {
	if rsp, ok := msg.(*rpc.SearchResponse); ok {
		value, ok := r.ctxs.Load(rsp.ID)
		if ok {
			r.ctxs.Delete(rsp.ID)
			value.(*asyncContext).done(rsp)
		}
	}
}

func (r *router) selectTargetPeer(db *meta.VectorDB) *meta.Peer {
	op := r.opts[db.ID]
	return db.Peers[int(atomic.AddUint64(&op, 1))%len(db.Peers)]
}
