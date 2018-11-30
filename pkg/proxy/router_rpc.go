package proxy

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/uuid"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

var (
	// ErrTimeout timeout
	ErrTimeout = errors.New("timeout")
)

type asyncContext struct {
	sync.Mutex
	wait         uint64
	completeC    chan struct{}
	to, received uint64
	dbs          []uint64
	distances    []float32
	ids          []int64
}

func (ctx *asyncContext) done(id interface{}, rsp *rpc.SearchResponse) {
	if nil != rsp {
		ctx.Lock()
		for idx, value := range rsp.Xids {
			if value != -1 && betterThan(rsp.Distances[idx], ctx.distances[idx]) {
				ctx.distances[idx] = rsp.Distances[idx]
				ctx.ids[idx] = value
				ctx.dbs[idx] = rsp.DB
			}
		}
		ctx.Unlock()
	}

	newV := atomic.AddUint64(&ctx.received, 1)
	if newV == atomic.LoadUint64(&ctx.to) && 0 == atomic.LoadUint64(&ctx.wait) {
		ctx.completeC <- struct{}{}
	}
}

func (ctx *asyncContext) get(timeout time.Duration) ([]uint64, []float32, []int64, error) {
	timeoutCtx, cancel := context.WithTimeout(context.TODO(), timeout)

	select {
	case <-ctx.completeC:
		cancel()
		return ctx.dbs, ctx.distances, ctx.ids, nil
	case <-timeoutCtx.Done():
		cancel()
		return ctx.dbs, ctx.distances, ctx.ids, ErrTimeout
	}
}

func (ctx *asyncContext) reset() {
	// reset counter
	ctx.wait = 0
	ctx.to = 0
	ctx.received = 0
	// clear chan
	close(ctx.completeC)
}

func (r *router) addAsyncCtx(ctx *asyncContext, req *rpc.SearchRequest) {
	id := key(req.ID)
	r.ctxs.Store(id, ctx)
	r.requests.Store(id, req)

	util.DefaultTimeoutWheel().Schedule(r.timeout, r.cleanTimeout, id)
}

func (r *router) cleanTimeout(id interface{}) {
	r.ctxs.Delete(id)
	r.requests.Delete(id)
}

func (r *router) search(req *rpc.SearchRequest) ([]uint64, []float32, []int64, error) {
	ctx := acquireCtx()

	l := len(req.Xq) / r.dim
	ctx.completeC = make(chan struct{}, 1)
	ctx.distances = make([]float32, l, l)
	ctx.ids = make([]int64, l, l)
	ctx.dbs = make([]uint64, l, l)
	for i := 0; i < l; i++ {
		ctx.dbs[i] = 0
		ctx.distances[i] = 0.0
		ctx.ids[i] = -1
	}

	r.foreach(func(db *meta.VectorDB, max bool) {
		ctx.to++
		bReq := &rpc.SearchRequest{
			ID:     uuid.NewV4().Bytes(),
			DB:     db.ID,
			Xq:     req.Xq,
			Offset: req.Offset,
			Last:   max,
		}
		r.addAsyncCtx(ctx, bReq)
		r.send(db, bReq)
	})

	dbs, ds, ids, err := ctx.get(r.timeout)
	releaseCtx(ctx)
	return dbs, ds, ids, err
}

func (r *router) onResponse(msg interface{}) {
	if rsp, ok := msg.(*rpc.SearchResponse); ok {
		id := key(rsp.ID)
		if value, ok := r.ctxs.Load(id); ok {
			r.ctxs.Delete(id)
			ctx := value.(*asyncContext)
			if rsp.SearchNext {
				r.sendWaitReq(id, ctx)
			}

			ctx.done(id, rsp)
		}
	} else if rsp, ok := msg.(*rpc.ErrResponse); ok {
		// choose next retry if err
		id := key(rsp.ID)
		if req, ok := r.requests.Load(id); ok {
			r.do(req.(*rpc.SearchRequest).DB, func(db *meta.VectorDB) {
				r.send(db, req.(*rpc.SearchRequest))
			})
		}
	}
}

func (r *router) send(db *meta.VectorDB, req *rpc.SearchRequest) {
	r.transports[r.selectTargetPeer(db).StoreID].sent(req)
}

func (r *router) sendWaitReq(id interface{}, ctx *asyncContext) {
	atomic.StoreUint64(&ctx.wait, 1)
	if req, ok := r.requests.Load(id); ok {
		r.waitC <- &waitReq{
			oldReq: req.(*rpc.SearchRequest),
			before: func(newReq *rpc.SearchRequest) {
				atomic.AddUint64(&ctx.to, 1)
			},
			after: func() {
				atomic.StoreUint64(&ctx.wait, 0)
			},
		}
	}
}

func (r *router) selectTargetPeer(db *meta.VectorDB) *meta.Peer {
	op := r.opts[db.ID]
	return db.Peers[int(atomic.AddUint64(&op, 1))%len(db.Peers)]
}

func key(id []byte) string {
	return hack.SliceToString(id)
}
