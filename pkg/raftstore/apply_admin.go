package raftstore

import (
	"errors"
	"fmt"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
)

func (d *applyDelegate) execAdminRequest(ctx *applyContext) (*execResult, error) {
	cmdType := ctx.req.Admin.Type
	switch cmdType {
	case raftpb.ChangePeer:
		return d.doExecChangePeer(ctx)
	case raftpb.Split:
		return d.doExecSplit(ctx)
	case raftpb.CompactLog:
		return d.doExecCompactLog(ctx)
	}

	return nil, nil
}

func (d *applyDelegate) doExecChangePeer(ctx *applyContext) (*execResult, error) {
	req := ctx.req.Admin.ChangePeer

	log.Infof("raftstore[db-%d]: exec change conf, type=<%s> epoch=<%+v>",
		d.db.ID,
		req.ChangeType.String(),
		d.db.Epoch)

	exists := findPeer(&d.db, req.Peer.StoreID)
	d.db.Epoch.ConfVersion++

	switch req.ChangeType {
	case etcdraftpb.ConfChangeAddNode:
		if exists != nil {
			return nil, nil
		}

		d.db.Peers = append(d.db.Peers, &req.Peer)
		log.Infof("raftstore[db-%d]: peer added, peer=<%+v>",
			d.db.ID,
			req.Peer)
	case etcdraftpb.ConfChangeRemoveNode:
		if exists == nil {
			return nil, nil
		}

		// Remove ourself, we will destroy all cell data later.
		// So we need not to apply following logs.
		if d.peerID == req.Peer.ID {
			d.setPendingRemove()
		}

		removePeer(&d.db, req.Peer.StoreID)

		log.Infof("raftstore[db-%d]: peer removed, peer=<%+v>",
			d.db.ID,
			req.Peer)
	}

	state := raftpb.Normal
	if d.isPendingRemove() {
		state = raftpb.Tombstone
	}

	err := d.ps.updatePeerState(d.db, state, ctx.wb)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: update db state failed, errors:\n %+v",
			d.db.ID,
			err)
	}

	ctx.resps = append(ctx.resps, &raftpb.AdminResponse{
		Type: raftpb.ChangePeer,
		ChangePeer: &raftpb.ChangePeerResponse{
			DB: d.db,
		},
	})

	result := &execResult{
		adminType: raftpb.ChangePeer,
		// confChange set by applyConfChange
		changePeer: &changePeer{
			peer: req.Peer,
			db:   d.db,
		},
	}

	return result, nil
}

func (d *applyDelegate) doExecSplit(ctx *applyContext) (*execResult, error) {
	req := ctx.req.Admin.Split

	if len(req.NewPeerIDs) != len(d.db.Peers) {
		log.Errorf("raftstore[db-%d]: invalid new peer id count, splitCount=<%d> currentCount=<%d>",
			d.db.ID,
			len(req.NewPeerIDs),
			len(d.db.Peers))

		return nil, nil
	}

	log.Infof("raftstore[db-%d]: exec split, db=<%+v>",
		d.db.ID,
		d.db)

	newDB := meta.VectorDB{
		ID:    req.NewID,
		Epoch: d.db.Epoch,
		State: meta.RWU,
	}

	for idx, id := range req.NewPeerIDs {
		newDB.Peers = append(newDB.Peers, &meta.Peer{
			ID:      id,
			StoreID: d.db.Peers[idx].StoreID,
		})
	}

	d.db.State = meta.RU
	d.db.Epoch.Version++
	newDB.Epoch.Version = d.db.Epoch.Version

	err := d.ps.updatePeerState(d.db, raftpb.Normal, ctx.wb)
	wb := d.ps.store.metaStore.NewWriteBatch()
	if err == nil {
		err = d.ps.updatePeerState(newDB, raftpb.Normal, wb)
	}
	if err == nil {
		err = d.ps.writeInitialState(newDB.ID, wb)
	}
	if err != nil {
		log.Fatalf("raftstore[db-%d]: save split db failed, newDB=<%+v> errors:\n %+v",
			d.db.ID,
			newDB,
			err)
	}

	err = d.ps.store.metaStore.Write(wb, false)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: commit split apply result failed, errors:\n %+v",
			d.db.ID,
			err)
	}

	ctx.resps = append(ctx.resps, &raftpb.AdminResponse{
		Type: raftpb.Split,
		Split: &raftpb.SplitResponse{
			OldDB: d.db,
			NewDB: newDB,
		},
	})

	result := &execResult{
		adminType: raftpb.Split,
		splitResult: &splitResult{
			oldDB: d.db,
			newDB: newDB,
		},
	}

	return result, nil
}

func (d *applyDelegate) doExecCompactLog(ctx *applyContext) (*execResult, error) {
	req := ctx.req.Admin.CompactLog

	compactIndex := req.CompactIndex
	firstIndex := ctx.applyState.TruncatedState.Index + 1

	if compactIndex <= firstIndex {
		log.Debugf("raftstore[db-%d]: no need to compact, compactIndex=<%d> firstIndex=<%d>",
			d.db.ID,
			compactIndex,
			firstIndex)
		return nil, nil
	}

	compactTerm := req.CompactTerm
	if compactTerm == 0 {
		log.Debugf("raftstore[db-%d]: compact term missing, skip, req=<%+v>",
			d.db.ID,
			req)
		return nil, errors.New("command format is outdated, please upgrade leader")
	}

	log.Debugf("raftstore[db-%d]: compact log entries to index %d",
		d.db.ID,
		compactIndex)
	if compactIndex <= ctx.applyState.TruncatedState.Index {
		return nil, errors.New("try to truncate compacted entries")
	} else if compactIndex > ctx.applyState.AppliedIndex {
		return nil, fmt.Errorf("compact index %d > applied index %d", compactIndex, ctx.applyState.AppliedIndex)
	}

	// we don't actually delete the logs now
	ctx.applyState.TruncatedState.Index = compactIndex
	ctx.applyState.TruncatedState.Term = compactTerm

	ctx.resps = append(ctx.resps, &raftpb.AdminResponse{
		Type:       raftpb.CompactLog,
		CompactLog: &raftpb.CompactLogResponse{},
	})

	result := &execResult{
		adminType: raftpb.CompactLog,
		compactResult: &compactLogResult{
			state:      ctx.applyState.TruncatedState,
			firstIndex: firstIndex,
		},
	}

	return result, nil
}
