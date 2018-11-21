package raftstore

import (
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

// HandleSearch handle search request
func (s *Store) HandleSearch(req *rpc.SearchRequest, cb func(interface{})) {
	log.Debugf("raftstore[db-%d]: search with offset %d", req.DB, req.Offset)

	pr := s.getDB(req.DB, false)
	if nil == pr {
		cb(errorDBNotFound(req.ID, req.DB))
		log.Debugf("raftstore[db-%d]: search with offset %d result not found", req.DB, req.Offset)
		return
	}

	// if the current db was become RU state, and the request offset is bigger,
	// client need send a search request to the new writable db
	ctx := acquireReqCtx()
	ctx.searchNext = pr.waitInsertCommitted(req)
	ctx.search = req
	ctx.msgType = int32(rpc.MsgSearchReq)
	ctx.cb = cb

	err := pr.addSearchRequest(ctx)
	if err != nil {
		releaseReqCtx(ctx)
		cb(errorOtherCMDResp(req.ID, err))
		return
	}
}

// HandleInsert handle insert request
func (s *Store) HandleInsert(req *rpc.InsertRequest, cb func(interface{})) {
	pr := s.getWritableDB(true)
	if nil == pr {
		cb(errorDBNotFound(req.ID, 0))
		return
	}

	ctx := acquireReqCtx()
	ctx.insert = req
	ctx.msgType = int32(rpc.MsgInsertReq)
	ctx.cb = cb

	err := pr.addRequest(ctx)
	if err != nil {
		releaseReqCtx(ctx)
		cb(errorOtherCMDResp(req.ID, err))
		return
	}
}

// HandleUpdate handle update request
func (s *Store) HandleUpdate(req *rpc.UpdateRequest, cb func(interface{})) {
	pr := s.getDB(req.DB, true)
	if nil == pr {
		cb(errorDBNotFound(req.ID, req.DB))
		return
	}

	ctx := acquireReqCtx()
	ctx.update = req
	ctx.msgType = int32(rpc.MsgUpdateReq)
	ctx.cb = cb

	err := pr.addRequest(ctx)
	if err != nil {
		releaseReqCtx(ctx)
		cb(errorOtherCMDResp(req.ID, err))
		return
	}
}
