package util

import (
	"sync"

	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

var (
	raftMessagePool    sync.Pool
	raftCMDRequestPool sync.Pool
	searchReqPool      sync.Pool
	searchRspPool      sync.Pool
	insertReqPool      sync.Pool
	insertRspPool      sync.Pool
	updateReqPool      sync.Pool
	updateRspPool      sync.Pool
	errRspPool         sync.Pool
)

// AcquireRaftMessage returns a raft message from pool
func AcquireRaftMessage() *raftpb.RaftMessage {
	v := raftMessagePool.Get()
	if v == nil {
		return &raftpb.RaftMessage{}
	}
	return v.(*raftpb.RaftMessage)
}

// ReleaseRaftMessage returns a raft message to pool
func ReleaseRaftMessage(value *raftpb.RaftMessage) {
	value.Reset()
	raftMessagePool.Put(value)
}

// AcquireRaftCMDRequest returns a raft cmd request from pool
func AcquireRaftCMDRequest() *raftpb.RaftCMDRequest {
	v := raftCMDRequestPool.Get()
	if v == nil {
		return &raftpb.RaftCMDRequest{}
	}
	return v.(*raftpb.RaftCMDRequest)
}

// ReleaseRaftCMDRequest returns a raft cmd request to pool
func ReleaseRaftCMDRequest(value *raftpb.RaftCMDRequest) {
	value.Reset()
	raftCMDRequestPool.Put(value)
}

// AcquireSearchReq returns a value from pool
func AcquireSearchReq() *rpc.SearchRequest {
	v := searchReqPool.Get()
	if v == nil {
		return &rpc.SearchRequest{}
	}
	return v.(*rpc.SearchRequest)
}

// ReleaseSearchReq returns a value to pool
func ReleaseSearchReq(value *rpc.SearchRequest) {
	value.Reset()
	searchReqPool.Put(value)
}

// AcquireSearchRsp returns a value from pool
func AcquireSearchRsp() *rpc.SearchResponse {
	v := searchRspPool.Get()
	if v == nil {
		return &rpc.SearchResponse{}
	}
	return v.(*rpc.SearchResponse)
}

// ReleaseSearchRsp returns a value to pool
func ReleaseSearchRsp(value *rpc.SearchResponse) {
	value.Reset()
	searchRspPool.Put(value)
}

// AcquireInsertReq returns a value from pool
func AcquireInsertReq() *rpc.InsertRequest {
	v := insertReqPool.Get()
	if v == nil {
		return &rpc.InsertRequest{}
	}
	return v.(*rpc.InsertRequest)
}

// ReleaseInsertReq returns a value to pool
func ReleaseInsertReq(value *rpc.InsertRequest) {
	value.Reset()
	insertReqPool.Put(value)
}

// AcquireInsertRsp returns a value from pool
func AcquireInsertRsp() *rpc.InsertResponse {
	v := insertRspPool.Get()
	if v == nil {
		return &rpc.InsertResponse{}
	}
	return v.(*rpc.InsertResponse)
}

// ReleaseInsertRsp returns a value to pool
func ReleaseInsertRsp(value *rpc.InsertResponse) {
	value.Reset()
	insertRspPool.Put(value)
}

// AcquireUpdateReq returns a value from pool
func AcquireUpdateReq() *rpc.UpdateRequest {
	v := updateReqPool.Get()
	if v == nil {
		return &rpc.UpdateRequest{}
	}
	return v.(*rpc.UpdateRequest)
}

// ReleaseUpdateReq returns a value to pool
func ReleaseUpdateReq(value *rpc.UpdateRequest) {
	value.Reset()
	updateReqPool.Put(value)
}

// AcquireUpdateRsp returns a value from pool
func AcquireUpdateRsp() *rpc.UpdateResponse {
	v := updateRspPool.Get()
	if v == nil {
		return &rpc.UpdateResponse{}
	}
	return v.(*rpc.UpdateResponse)
}

// ReleaseUpdateRsp returns a value to pool
func ReleaseUpdateRsp(value *rpc.UpdateResponse) {
	value.Reset()
	updateRspPool.Put(value)
}

// AcquireErrorRsp returns a value from pool
func AcquireErrorRsp() *rpc.ErrResponse {
	v := errRspPool.Get()
	if v == nil {
		return &rpc.ErrResponse{}
	}
	return v.(*rpc.ErrResponse)
}

// ReleaseErrorRsp returns a value to pool
func ReleaseErrorRsp(value *rpc.ErrResponse) {
	value.Reset()
	errRspPool.Put(value)
}
