package server

import (
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

func (s *Server) handleSearch(req *rpc.SearchRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleSearch(req, s.handleResponse, s.handleErrResponse)
}

func (s *Server) handleInsert(req *rpc.InsertRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleInsert(req, s.handleResponse, s.handleErrResponse)
}

func (s *Server) handleUpdate(req *rpc.UpdateRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleUpdate(req, s.handleResponse, s.handleErrResponse)
}

func (s *Server) handleResponse(rsp interface{}) {
	var sessionID interface{}

	if value, ok := rsp.(*rpc.InsertResponse); ok {
		sessionID, _ = s.routing.Load(hack.SliceToString(value.ID))
	} else if value, ok := rsp.(*rpc.UpdateResponse); ok {
		sessionID, _ = s.routing.Load(hack.SliceToString(value.ID))
	} else if value, ok := rsp.(*rpc.SearchResponse); ok {
		sessionID, _ = s.routing.Load(hack.SliceToString(value.ID))
	} else {
		log.Fatalf("bugs:not support response %+v", rsp)
	}

	if nil != sessionID {
		s.doResp(sessionID, rsp)
	}
}

func (s *Server) handleErrResponse(uuid []byte, err *raftpb.Error) {
	s.doResp(hack.SliceToString(uuid), err)
}

func (s *Server) doResp(id interface{}, value interface{}) {
	sc, ok := s.sessions.Load(id)
	if !ok {
		return
	}

	sc.(*session).resp(value)
}
