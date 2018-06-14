package raftstore

import (
	"errors"

	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
)

var (
	errStaleCMD           = errors.New("stale command")
	errStaleEpoch         = errors.New("stale epoch")
	errNotLeader          = errors.New("NotLeader")
	errDBNotFound         = errors.New("db not found")
	errMissingUUIDCMD     = errors.New("missing request uuid")
	errLargeRaftEntrySize = errors.New("entry is too large")
	errStoreNotMatch      = errors.New("key not in store")

	infoStaleCMD  = new(raftpb.StaleCommand)
	storeNotMatch = new(raftpb.StoreNotMatch)
)

func errorBaseResp() *raftpb.Error {
	return &raftpb.Error{}
}

func errorDBNotFound(id uint64) *raftpb.Error {
	return &raftpb.Error{
		Message: errDBNotFound.Error(),
		DbNotFound: &raftpb.DBNotFound{
			ID: id,
		},
	}
}

func errorStoreNotMatch() *raftpb.Error {
	return &raftpb.Error{
		Message:       errStoreNotMatch.Error(),
		StoreNotMatch: storeNotMatch,
	}
}

func errorOtherCMDResp(err error) *raftpb.Error {
	errRsp := errorBaseResp()
	errRsp.Message = err.Error()
	return errRsp
}

func errorStaleCMDResp() *raftpb.Error {
	resp := errorBaseResp()
	resp.Message = errStaleCMD.Error()
	resp.StaleCommand = infoStaleCMD

	return resp
}

func errorNotLeader(id uint64, leader *meta.Peer) *raftpb.Error {
	resp := errorBaseResp()
	resp.Message = errNotLeader.Error()
	resp.NotLeader = &raftpb.NotLeader{
		ID: id,
	}
	if leader != nil {
		resp.NotLeader.Leader = *leader
	}

	return resp
}

func errorLargeRaftEntrySize(id uint64, size uint64) *raftpb.Error {
	resp := errorBaseResp()
	resp.Message = errLargeRaftEntrySize.Error()
	resp.RaftEntryTooLarge = &raftpb.RaftEntryTooLarge{
		ID:        id,
		EntrySize: size,
	}

	return resp
}

func errorStaleEpochResp(newDBs ...meta.VectorDB) *raftpb.Error {
	resp := errorBaseResp()
	resp.Message = errStaleCMD.Error()
	resp.StaleEpoch = &raftpb.StaleEpoch{
		NewDBs: newDBs,
	}

	return resp
}
