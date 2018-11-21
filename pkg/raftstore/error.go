package raftstore

import (
	"errors"

	"github.com/infinivision/hyena/pkg/pb/meta"
	rpcpb "github.com/infinivision/hyena/pkg/pb/rpc"
)

var (
	errStaleCMD           = errors.New("stale command")
	errStaleEpoch         = errors.New("stale epoch")
	errNotLeader          = errors.New("NotLeader")
	errDBNotFound         = errors.New("db not found")
	errMissingUUIDCMD     = errors.New("missing request uuid")
	errLargeRaftEntrySize = errors.New("entry is too large")
	errStoreNotMatch      = errors.New("key not in store")

	infoStaleCMD  = new(rpcpb.StaleCommand)
	storeNotMatch = new(rpcpb.StoreNotMatch)
)

func copyWithID(uuid []byte, source *rpcpb.ErrResponse) *rpcpb.ErrResponse {
	value := &rpcpb.ErrResponse{}
	*value = *source
	value.ID = uuid

	return value
}

func errorBaseResp(uuid []byte) *rpcpb.ErrResponse {
	return &rpcpb.ErrResponse{
		ID: uuid,
	}
}

func errorDBNotFound(uuid []byte, id uint64) *rpcpb.ErrResponse {
	errRsp := errorBaseResp(uuid)
	errRsp.Message = errDBNotFound.Error()
	errRsp.DBNotFound = &rpcpb.DBNotFound{
		ID: id,
	}

	return errRsp
}

func errorStoreNotMatch(uuid []byte) *rpcpb.ErrResponse {
	errRsp := errorBaseResp(uuid)
	errRsp.Message = errStoreNotMatch.Error()
	errRsp.StoreNotMatch = storeNotMatch

	return errRsp
}

func errorOtherCMDResp(uuid []byte, err error) *rpcpb.ErrResponse {
	errRsp := errorBaseResp(uuid)
	errRsp.Message = err.Error()
	return errRsp
}

func errorStaleCMDResp(uuid []byte) *rpcpb.ErrResponse {
	resp := errorBaseResp(uuid)
	resp.Message = errStaleCMD.Error()
	resp.StaleCommand = infoStaleCMD

	return resp
}

func errorNotLeader(uuid []byte, id uint64, leader *meta.Peer) *rpcpb.ErrResponse {
	resp := errorBaseResp(uuid)
	resp.Message = errNotLeader.Error()
	resp.NotLeader = &rpcpb.NotLeader{
		ID: id,
	}
	if leader != nil {
		resp.NotLeader.Leader = *leader
	}

	return resp
}

func errorLargeRaftEntrySize(uuid []byte, id uint64, size uint64) *rpcpb.ErrResponse {
	resp := errorBaseResp(uuid)
	resp.Message = errLargeRaftEntrySize.Error()
	resp.RaftEntryTooLarge = &rpcpb.RaftEntryTooLarge{
		ID:        id,
		EntrySize: size,
	}

	return resp
}

func errorStaleEpochResp(uuid []byte, newDBs ...meta.VectorDB) *rpcpb.ErrResponse {
	resp := errorBaseResp(uuid)
	resp.Message = errStaleCMD.Error()
	resp.StaleEpoch = &rpcpb.StaleEpoch{
		NewDBs: newDBs,
	}

	return resp
}
