package raftstore

import (
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
)

func findPeer(db *meta.VectorDB, storeID uint64) *meta.Peer {
	for _, peer := range db.Peers {
		if peer.StoreID == storeID {
			return peer
		}
	}

	return nil
}

func removePeer(db *meta.VectorDB, storeID uint64) {
	var newPeers []*meta.Peer
	for _, peer := range db.Peers {
		if peer.StoreID != storeID {
			newPeers = append(newPeers, peer)
		}
	}

	db.Peers = newPeers
}

func newPeer(peerID, storeID uint64) meta.Peer {
	return meta.Peer{
		ID:      peerID,
		StoreID: storeID,
	}
}

func removedPeers(new, old meta.VectorDB) []uint64 {
	var ids []uint64

	for _, o := range old.Peers {
		c := 0
		for _, n := range new.Peers {
			if n.ID == o.ID {
				c++
				break
			}
		}

		if c == 0 {
			ids = append(ids, o.ID)
		}
	}

	return ids
}

func checkEpoch(db meta.VectorDB, req *raftpb.RaftCMDRequest) bool {
	checkVer := false
	checkConfVer := false

	if req.Admin != nil {
		switch req.Admin.Type {
		case raftpb.Split:
			checkVer = true
		case raftpb.ChangePeer:
			checkConfVer = true
		case raftpb.TransferLeader:
			checkVer = true
			checkConfVer = true
		}
	} else {
		// for other command, we don't care conf version.
		checkVer = true
	}

	if !checkConfVer && !checkVer {
		return true
	}

	fromEpoch := req.Header.Epoch
	lastestEpoch := db.Epoch

	if (checkConfVer && fromEpoch.ConfVersion < lastestEpoch.ConfVersion) ||
		(checkVer && fromEpoch.Version < lastestEpoch.Version) {
		log.Infof("raftstore[db-%d]: reveiced stale epoch, lastest=<%v> reveived=<%v>",
			db.ID,
			lastestEpoch,
			fromEpoch)
		return false
	}

	return true
}

// check whether epoch is staler than checkEpoch.
func isEpochStale(epoch, checkEpoch meta.Epoch) bool {
	return epoch.Version < checkEpoch.Version ||
		epoch.ConfVersion < checkEpoch.ConfVersion
}
