package raftstore

import (
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

// HandleSearch handle search request
func (s *Store) HandleSearch(req *rpc.SearchRequest, cb func(interface{}), cbErr func([]byte, *raftpb.Error)) {
	pr := s.getDB(req.DB, false)
	if nil == pr {
		cbErr(req.ID, errorDBNotFound(0))
		return
	}

	pr.onSearch(req, cb, cbErr)
}

// HandleInsert handle insert request
func (s *Store) HandleInsert(req *rpc.InsertRequest, cb func(interface{}), cbErr func([]byte, *raftpb.Error)) {
	pr := s.getWriteableDB(true)
	if nil == pr {
		cbErr(req.ID, errorDBNotFound(0))
		return
	}

	ctx := acquireReqCtx()
	ctx.insert = req
	ctx.msgType = int32(rpc.MsgInsertReq)

	err := pr.addRequest(ctx)
	if err != nil {
		releaseReqCtx(ctx)
		cbErr(req.ID, errorOtherCMDResp(err))
		return
	}
}

// HandleUpdate handle update request
func (s *Store) HandleUpdate(req *rpc.UpdateRequest, cb func(interface{}), cbErr func([]byte, *raftpb.Error)) {
	pr := s.getDB(req.DB, true)
	if nil == pr {
		cbErr(req.ID, errorDBNotFound(req.DB))
		return
	}

	ctx := acquireReqCtx()
	ctx.update = req
	ctx.msgType = int32(rpc.MsgUpdateReq)
	err := pr.addRequest(ctx)

	if err != nil {
		releaseReqCtx(ctx)
		cbErr(req.ID, errorOtherCMDResp(err))
		return
	}
}
