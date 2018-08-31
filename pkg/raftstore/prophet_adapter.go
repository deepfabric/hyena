package raftstore

import (
	"time"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/meta"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/prophet"
)

// ProphetAdapter adapter prophet
type ProphetAdapter struct {
	store *Store
}

// NewResource return a new resource
func (pa *ProphetAdapter) NewResource() prophet.Resource {
	return &ResourceAdapter{}
}

// NewContainer return a new container
func (pa *ProphetAdapter) NewContainer() prophet.Container {
	return &ContainerAdapter{}
}

// FetchResourceHB fetch resource HB
func (pa *ProphetAdapter) FetchResourceHB(id uint64) *prophet.ResourceHeartbeatReq {
	pr := pa.store.getDB(id, false)
	if pr == nil {
		log.Fatal("bug: missing replica")
	}

	return getResourceHB(pr)
}

// FetchAllResourceHB fetch all resource HB
func (pa *ProphetAdapter) FetchAllResourceHB() []*prophet.ResourceHeartbeatReq {
	var values []*prophet.ResourceHeartbeatReq
	pa.store.replicates.Range(func(key, value interface{}) bool {
		pr := value.(*PeerReplicate)
		pr.checkPeers()

		if pr.isLeader() {
			values = append(values, getResourceHB(pr))
		}

		return true
	})

	return values
}

// FetchContainerHB fetch container HB
func (pa *ProphetAdapter) FetchContainerHB() *prophet.ContainerHeartbeatReq {
	stats, err := util.DiskStats(pa.store.cfg.DataPath)
	if err != nil {
		log.Errorf("raftstore: fetch store heartbeat failed, errors:\n %+v",
			pa.store.meta.ID,
			err)
		return nil
	}

	st := pa.store.getDBStatus()
	req := new(prophet.ContainerHeartbeatReq)
	req.Container = &ContainerAdapter{meta: pa.store.meta}
	req.StorageCapacity = stats.Total
	req.StorageAvailable = stats.Free
	req.ApplyingSnapCount = st.applyingSnapCount
	req.LeaderCount = st.dbLeaderCount
	req.ReplicaCount = st.dbCount
	// TODO: impl
	req.Busy = false
	req.ReceivingSnapCount = 0
	req.SendingSnapCount = 0

	return req
}

// ResourceHBInterval fetch resource HB interface
func (pa *ProphetAdapter) ResourceHBInterval() time.Duration {
	return pa.store.cfg.DBHBInterval
}

// ContainerHBInterval fetch container HB interface
func (pa *ProphetAdapter) ContainerHBInterval() time.Duration {
	return pa.store.cfg.StoreHBInterval
}

// HBHandler HB hander
func (pa *ProphetAdapter) HBHandler() prophet.HeartbeatHandler {
	return pa
}

// ChangeLeader prophet adapter
func (pa *ProphetAdapter) ChangeLeader(resourceID uint64, newLeader *prophet.Peer) {
	pr := pa.store.getDB(resourceID, true)
	if nil == pr {
		log.Fatal("bug: db can'not be nil")
	}

	log.Infof("raftstore[db-%d]: try to transfer leader, from=<%v> to=<%+v>",
		pr.id,
		pr.id,
		newLeader.ID)

	pr.onAdmin(&raftpb.AdminRequest{
		Type: raftpb.TransferLeader,
		TransferLeader: &raftpb.TransferLeaderRequest{
			Peer: meta.Peer{
				ID:      newLeader.ID,
				StoreID: newLeader.ContainerID,
			},
		},
	})
}

// ChangePeer prophet adapter
func (pa *ProphetAdapter) ChangePeer(resourceID uint64, peer *prophet.Peer, changeType prophet.ChangePeerType) {
	pr := pa.store.getDB(resourceID, true)
	if nil == pr {
		log.Fatal("bug: db can'not be nil")
	}

	var ct etcdraftpb.ConfChangeType
	if changeType == prophet.AddPeer {
		ct = etcdraftpb.ConfChangeAddNode
	} else if changeType == prophet.RemovePeer {
		ct = etcdraftpb.ConfChangeRemoveNode
	}

	p := meta.Peer{
		ID:      peer.ID,
		StoreID: peer.ContainerID,
	}

	log.Infof("raftstore[db-%d]: try to change peer, type=<%s> peer=<%v>",
		pr.id,
		ct.String(),
		p)

	pr.onAdmin(&raftpb.AdminRequest{
		Type: raftpb.ChangePeer,
		ChangePeer: &raftpb.ChangePeerRequest{
			ChangeType: ct,
			Peer:       p,
		},
	})
}

// ContainerAdapter adapter for prophet's container and store
type ContainerAdapter struct {
	meta meta.Store
}

// ID adapter prophet
func (c *ContainerAdapter) ID() uint64 {
	return c.meta.ID
}

// Lables adapter prophet
func (c *ContainerAdapter) Lables() []prophet.Pair {
	var pairs []prophet.Pair
	for _, l := range c.meta.Lables {
		pairs = append(pairs, prophet.Pair{
			Key:   l.Key,
			Value: l.Value,
		})
	}

	return pairs
}

// State adapter prophet
func (c *ContainerAdapter) State() prophet.State {
	switch c.meta.State {
	case meta.UP:
		return prophet.UP
	case meta.Down:
		return prophet.Down
	case meta.Tombstone:
		return prophet.Tombstone
	}

	return prophet.Tombstone
}

// Clone adapter prophet
func (c *ContainerAdapter) Clone() prophet.Container {
	value := &ContainerAdapter{}
	pbutil.MustUnmarshal(&value.meta, pbutil.MustMarshal(&c.meta))
	return value
}

// Marshal adapter prophet
func (c *ContainerAdapter) Marshal() ([]byte, error) {
	return (&c.meta).Marshal()
}

// Unmarshal adapter prophet
func (c *ContainerAdapter) Unmarshal(data []byte) error {
	return (&c.meta).Unmarshal(data)
}

// ResourceAdapter adapter for prophet's resource and db
type ResourceAdapter struct {
	meta meta.VectorDB
}

// ID adapter prophet
func (r *ResourceAdapter) ID() uint64 {
	return r.meta.ID
}

// Peers adapter prophet
func (r *ResourceAdapter) Peers() []*prophet.Peer {
	var values []*prophet.Peer
	for _, p := range r.meta.Peers {
		values = append(values, &prophet.Peer{
			ID:          p.ID,
			ContainerID: p.StoreID,
		})
	}

	return values
}

// SetPeers adapter prophet
func (r *ResourceAdapter) SetPeers(peers []*prophet.Peer) {
	var values []*meta.Peer
	for _, p := range peers {
		values = append(values, &meta.Peer{
			ID:      p.ID,
			StoreID: p.ContainerID,
		})
	}
	r.meta.Peers = values
}

// Stale adapter prophet
func (r *ResourceAdapter) Stale(other prophet.Resource) bool {
	otherDB := other.(*ResourceAdapter)
	return isEpochStale(otherDB.meta.Epoch, r.meta.Epoch)
}

// Changed adapter prophet
func (r *ResourceAdapter) Changed(other prophet.Resource) bool {
	otherEpoch := other.(*ResourceAdapter).meta.Epoch

	return otherEpoch.Version > r.meta.Epoch.Version ||
		otherEpoch.ConfVersion > r.meta.Epoch.ConfVersion
}

// Clone adapter prophet
func (r *ResourceAdapter) Clone() prophet.Resource {
	value := &ResourceAdapter{}
	pbutil.MustUnmarshal(&value.meta, pbutil.MustMarshal(&r.meta))
	return value
}

// Marshal adapter prophet
func (r *ResourceAdapter) Marshal() ([]byte, error) {
	return (&r.meta).Marshal()
}

// Unmarshal adapter prophet
func (r *ResourceAdapter) Unmarshal(data []byte) error {
	return (&r.meta).Unmarshal(data)
}

func getResourceHB(pr *PeerReplicate) *prophet.ResourceHeartbeatReq {
	req := new(prophet.ResourceHeartbeatReq)
	req.Resource = &ResourceAdapter{meta: pr.ps.db}
	req.LeaderPeer = &prophet.Peer{ID: pr.peer.ID, ContainerID: pr.peer.StoreID}
	req.DownPeers = pr.collectDownPeers(pr.store.cfg.MaxPeerDownTime)
	req.PendingPeers = pr.collectPendingPeers()
	log.Infof("raftstore[db-%d]: db: %+v, leader: %+v",
		pr.id,
		req.Resource,
		req.LeaderPeer)
	return req
}
