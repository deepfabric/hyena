package proxy

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/fagongzi/util/uuid"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

type asyncContext struct {
	sync.Mutex
	timeouts     sync.Map
	completeC    chan struct{}
	to, received uint64
	db           uint64
	distances    []float32
	ids          []int64
}

func (ctx *asyncContext) addTimeout(id interface{}, timeout time.Duration) {
	t, err := util.DefaultTimeoutWheel().Schedule(timeout, ctx.onTimeout, id)
	if err != nil {
		log.Fatal("bug: timeout can not added failed")
	}
	ctx.timeouts.Store(id, &t)
}

func (ctx *asyncContext) done(id interface{}, rsp *rpc.SearchResponse) {
	if value, ok := ctx.timeouts.Load(id); ok {
		ctx.timeouts.Delete(id)
		value.(*goetty.Timeout).Stop()
		if nil != rsp {
			ctx.Lock()
			for idx, value := range rsp.Xids {
				if value != -1 && betterThan(rsp.Distances[idx], ctx.distances[idx]) {
					ctx.distances[idx] = rsp.Distances[idx]
					ctx.ids[idx] = value
					ctx.db = rsp.DB
				}
			}
			ctx.Unlock()
		}
	}

	newV := atomic.AddUint64(&ctx.received, 1)
	if newV == ctx.to {
		ctx.completeC <- struct{}{}
	}
}

func (ctx *asyncContext) onTimeout(id interface{}) {
	if _, ok := ctx.timeouts.Load(id); ok {
		ctx.done(id, nil)
	}
}

func (ctx *asyncContext) get(timeout time.Duration) (uint64, []float32, []int64, error) {
	<-ctx.completeC
	return ctx.db, ctx.distances, ctx.ids, nil
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
		default:
			close(ctx.completeC)
			return
		}
	}
}

func (r *router) addAsyncCtx(ctx *asyncContext, req *rpc.SearchRequest) {
	id := key(req.ID)
	ctx.addTimeout(id, r.timeout)
	r.ctxs.Store(id, ctx)
}

func (r *router) search(req *rpc.SearchRequest) (uint64, []float32, []int64, error) {
	ctx := acquireCtx()

	r.RLock()
	l := len(req.Xq) / r.dim
	ctx.to = uint64(len(r.dbs))
	ctx.completeC = make(chan struct{}, 1)
	ctx.db = 0
	ctx.distances = make([]float32, l, l)
	ctx.ids = make([]int64, l, l)
	for i := 0; i < l; i++ {
		ctx.distances[i] = 0.0
		ctx.ids[i] = -1
	}

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

	db, ds, ids, err := ctx.get(r.timeout)
	releaseCtx(ctx)
	return db, ds, ids, err
}

func (r *router) onResponse(msg interface{}) {
	if rsp, ok := msg.(*rpc.SearchResponse); ok {
		id := key(rsp.ID)
		value, ok := r.ctxs.Load(id)
		if ok {
			r.ctxs.Delete(id)
			value.(*asyncContext).done(id, rsp)
		}
	} else if rsp, ok := msg.(*rpc.ErrResponse); ok {
		id := key(rsp.ID)
		value, ok := r.ctxs.Load(id)
		if ok {
			r.ctxs.Delete(id)
			value.(*asyncContext).done(id, nil)
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
