package raftstore

import (
	"errors"
	"fmt"
	"io"
	"sync"

	etcdraftpb "github.com/coreos/etcd/raft/raftpb"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	raftpb "github.com/infinivision/hyena/pkg/pb/raft"
	"github.com/infinivision/hyena/pkg/util"
)

var (
	errConnect = errors.New("not connected")
)

type transport struct {
	sync.RWMutex

	store             *Store
	server            *goetty.Server
	storeAddrDetector func(storeID uint64) (string, error)
	handler           func(interface{})
	addrs             *sync.Map
	addrsRevert       *sync.Map
	conns             map[uint64]goetty.IOSessionPool
	mask              uint64
	snapMask          uint64
	rafts             []*util.Queue
	snaps             []*util.Queue
}

func newTransport(store *Store, handler func(interface{})) *transport {
	t := &transport{
		server: goetty.NewServer(store.meta.Address,
			goetty.WithServerDecoder(decoder),
			goetty.WithServerEncoder(encoder)),
		conns:       make(map[uint64]goetty.IOSessionPool),
		addrs:       &sync.Map{},
		addrsRevert: &sync.Map{},
		rafts:       make([]*util.Queue, store.cfg.SentRaftWorkerCount, store.cfg.SentRaftWorkerCount),
		snaps:       make([]*util.Queue, store.cfg.SentSnapWorkerCount, store.cfg.SentSnapWorkerCount),
		mask:        store.cfg.SentRaftWorkerCount - 1,
		snapMask:    store.cfg.SentSnapWorkerCount - 1,
		handler:     handler,
		store:       store,
	}

	t.storeAddrDetector = t.getStoreAddr

	return t
}

func (t *transport) start() error {
	for i := uint64(0); i < t.store.cfg.SentRaftWorkerCount; i++ {
		t.rafts[i] = &util.Queue{}
		go t.readyToSendRaft(t.rafts[i])
	}

	for i := uint64(0); i < t.store.cfg.SentSnapWorkerCount; i++ {
		t.snaps[i] = &util.Queue{}
		go t.readyToSendSnapshots(t.snaps[i])
	}

	return t.server.Start(t.doConnection)
}

func (t *transport) stop() {
	for _, q := range t.snaps {
		q.Dispose()
	}

	for _, q := range t.rafts {
		q.Dispose()
	}

	t.server.Stop()
	log.Infof("raftstore: transfer stopped")
}

func (t *transport) doConnection(session goetty.IOSession) error {
	remoteIP := session.RemoteIP()

	log.Infof("raftstore: %s connected", remoteIP)
	for {
		msg, err := session.Read()
		if err != nil {
			if err == io.EOF {
				log.Infof("raftstore: closed by %s", remoteIP)
			} else {
				log.Warnf("raftstore: read error from conn-%s, errors:\n%+v", remoteIP, err)
			}

			return err
		}

		t.handler(msg)
	}
}

func (t *transport) sendRaftMessage(msg *raftpb.RaftMessage) {
	storeID := msg.To.StoreID

	if storeID == t.store.meta.ID {
		t.store.notify(msg)
		return
	}

	if msg.Message.Type == etcdraftpb.MsgSnap {
		snapMsg := &raftpb.SnapshotMessage{}
		pbutil.MustUnmarshal(snapMsg, msg.Message.Snapshot.Data)
		snapMsg.Header.From = msg.From
		snapMsg.Header.To = msg.To
		t.snaps[t.snapMask&storeID].Put(snapMsg)
	}

	t.rafts[t.mask&storeID].Put(msg)
}

func (t *transport) readyToSendRaft(q *util.Queue) {
	items := make([]interface{}, batch, batch)

	for {
		n, err := q.Get(batch, items)
		if err != nil {
			log.Infof("raftstore: transfer sent worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*raftpb.RaftMessage)
			err := t.doSend(msg, msg.To.StoreID)
			t.postSend(msg, err)
		}
	}
}

func (t *transport) readyToSendSnapshots(q *util.Queue) {
	items := make([]interface{}, batch, batch)

	for {
		n, err := q.Get(batch, items)
		if err != nil {
			log.Infof("raftstore: raft transfer send snapshot worker stopped")
			return
		}

		for i := int64(0); i < n; i++ {
			msg := items[i].(*raftpb.SnapshotMessage)
			id := msg.Header.To.StoreID

			conn, err := t.getConn(id)
			if err != nil {
				log.Errorf("raftstore: create conn to %d failed, errors:\n%v",
					id,
					err)
				continue
			}

			err = t.doSendSnap(msg, conn)
			t.putConn(id, conn)

			if err != nil {
				log.Errorf("raftstore: send snap failed, snap=<%+v>,errors:\n%v",
					msg,
					err)
				continue
			}
		}
	}
}

func (t *transport) doSendSnap(msg *raftpb.SnapshotMessage, conn goetty.IOSession) error {
	if t.store.snapMgr.Register(msg, sending) {
		defer t.store.snapMgr.Deregister(msg, sending)

		log.Infof("raftstore[db-%d]: start send pending snap, term=<%d> index=<%d>",
			msg.Header.DB.ID,
			msg.Header.Term,
			msg.Header.Index)

		if !t.store.snapMgr.Exists(msg) {
			return fmt.Errorf("raftstore[db-%d]: missing snapshot file, header=<%+v>",
				msg.Header.DB.ID,
				msg.Header)
		}

		size, err := t.store.snapMgr.WriteTo(msg, conn)
		if err != nil {
			conn.Close()
			return err
		}

		log.Infof("raftstore[db-%d]: pending snap sent succ, size=<%d>, epoch=<%s> term=<%d> index=<%d>",
			msg.Header.DB.ID,
			size,
			msg.Header.DB.Epoch.String(),
			msg.Header.Term,
			msg.Header.Index)
	}

	return nil
}

func (t *transport) postSend(msg *raftpb.RaftMessage, err error) {
	if err != nil {
		log.Errorf("raftstore[db-%d]: send msg from %d to %d failure, errors:\n%s",
			msg.ID,
			msg.From.ID,
			msg.To.ID,
			err)

		pr := t.store.getDB(msg.ID, true)
		if pr != nil {
			pr.report(msg.Message)
		}
	}

	util.ReleaseRaftMessage(msg)
}

func (t *transport) doSend(msg interface{}, to uint64) error {
	conn, err := t.getConn(to)
	if err != nil {
		return err
	}

	err = t.doWrite(msg, conn)
	t.putConn(to, conn)
	return err
}

func (t *transport) doWrite(msg interface{}, conn goetty.IOSession) error {
	err := conn.WriteAndFlush(msg)
	if err != nil {
		conn.Close()
		return err
	}

	return nil
}

func (t *transport) getConn(storeID uint64) (goetty.IOSession, error) {
	conn, err := t.getConnLocked(storeID)
	if err != nil {
		return nil, err
	}

	if t.checkConnect(storeID, conn) {
		return conn, nil
	}

	t.putConn(storeID, conn)
	return nil, errConnect
}

func (t *transport) putConn(id uint64, conn goetty.IOSession) {
	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool != nil {
		pool.Put(conn)
	} else {
		conn.Close()
	}
}

func (t *transport) getConnLocked(id uint64) (goetty.IOSession, error) {
	var err error

	t.RLock()
	pool := t.conns[id]
	t.RUnlock()

	if pool == nil {
		t.Lock()
		pool = t.conns[id]
		if pool == nil {
			pool, err = goetty.NewIOSessionPool(1, 2, func() (goetty.IOSession, error) {
				return t.createConn(id)
			})

			if err != nil {
				return nil, err
			}

			t.conns[id] = pool
		}
		t.Unlock()
	}

	return pool.Get()
}

func (t *transport) checkConnect(id uint64, conn goetty.IOSession) bool {
	if nil == conn {
		return false
	}

	if conn.IsConnected() {
		return true
	}

	ok, err := conn.Connect()
	if err != nil {
		log.Errorf("raftstore: connect to store %d failure, errors:\n %+v",
			id,
			err)
		return false
	}

	log.Infof("raftstore: connected to store %d", id)
	return ok
}

func (t *transport) createConn(id uint64) (goetty.IOSession, error) {
	addr, err := t.storeAddrDetector(id)
	if err != nil {
		return nil, err
	}

	return goetty.NewConnector(addr,
		goetty.WithClientDecoder(decoder),
		goetty.WithClientEncoder(encoder)), nil
}

func (t *transport) getStoreAddr(storeID uint64) (string, error) {
	addr, ok := t.addrs.Load(storeID)
	if !ok {
		c, err := t.store.pd.GetStore().GetContainer(storeID)
		if err != nil {
			return "", err
		}

		addr = c.(*ContainerAdapter).meta.Address
		t.addrs.Store(storeID, addr)
		t.addrsRevert.Store(addr, storeID)
	}

	return addr.(string), nil
}
