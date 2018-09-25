package proxy

import (
	"sync"

	"github.com/infinivision/hyena/pkg/pb/rpc"
)

var (
	ctxPool = sync.Pool{
		New: func() interface{} {
			return &asyncContext{}
		},
	}

	reqPool = sync.Pool{
		New: func() interface{} {
			return &rpc.SearchRequest{}
		},
	}
)

func acquireCtx() *asyncContext {
	return ctxPool.Get().(*asyncContext)
}

func releaseCtx(value *asyncContext) {
	value.reset()
	ctxPool.Put(value)
}

func acquireRequest() *rpc.SearchRequest {
	return reqPool.Get().(*rpc.SearchRequest)
}

func releaseRequest(value *rpc.SearchRequest) {
	value.Reset()
	reqPool.Put(value)
}
