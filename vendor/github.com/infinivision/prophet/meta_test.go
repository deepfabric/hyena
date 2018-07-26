package prophet

import (
	"testing"
)

func TestPeerClone(t *testing.T) {
	old := newTestPeer(1, 10)
	cloned := old.Clone()

	if cloned.ID != old.ID {
		t.Error("peer clone failed with ID")
	}
	if cloned.ContainerID != old.ContainerID {
		t.Error("peer clone failed with ContainerID")
	}

	old.ID = 2
	old.ContainerID = 20
	if cloned.ID == old.ID {
		t.Error("peer clone failed with ID changed")
	}
	if cloned.ContainerID == old.ContainerID {
		t.Error("peer clone failed with ContainerID changed")
	}
}

func TestPeerStatusClone(t *testing.T) {
	old := &PeerStats{
		Peer:        newTestPeer(1, 10),
		DownSeconds: 10,
	}
	cloned := old.Clone()

	if cloned.DownSeconds != old.DownSeconds {
		t.Error("peer status clone failed with DownSeconds")
	}

	old.DownSeconds = 20

	if cloned.DownSeconds == old.DownSeconds {
		t.Error("peer status clone failed with DownSeconds changed")
	}
}

func newTestPeer(id, containerID uint64) *Peer {
	return &Peer{
		ID:          id,
		ContainerID: containerID,
	}
}
