package server

import (
	"github.com/fagongzi/log"
	"github.com/fagongzi/util/hack"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

func (s *Server) handleSearch(req *rpc.SearchRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleSearch(req, s.handleResponse)
}

func (s *Server) handleInsert(req *rpc.InsertRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleInsert(req, s.handleResponse)
}

func (s *Server) handleUpdate(req *rpc.UpdateRequest, sessionID interface{}) {
	s.routing.Store(hack.SliceToString((req.ID)), sessionID)
	s.store.HandleUpdate(req, s.handleResponse)
}

func (s *Server) handleResponse(rsp interface{}) {
	var id interface{}
	if value, ok := rsp.(*rpc.InsertResponse); ok {
		id = hack.SliceToString(value.ID)
	} else if value, ok := rsp.(*rpc.UpdateResponse); ok {
		id = hack.SliceToString(value.ID)
	} else if value, ok := rsp.(*rpc.SearchResponse); ok {
		id = hack.SliceToString(value.ID)
	} else if value, ok := rsp.(*rpc.ErrResponse); ok {
		id = hack.SliceToString(value.ID)
	} else {
		log.Fatalf("bugs:not support response %+v", rsp)
	}

	if sessionID, ok := s.routing.Load(id); ok {
		s.routing.Delete(id)
		s.doResp(sessionID, rsp)
	}
}

func (s *Server) doResp(id interface{}, value interface{}) {
	sc, ok := s.sessions.Load(id)
	if !ok {
		return
	}

	sc.(*session).resp(value)
}
