package proxy

import (
	"sync"
	"time"

	"github.com/infinivision/hyena/pkg/pb/rpc"

	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/fagongzi/util/uuid"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/prophet"
)

type router struct {
	sync.RWMutex

	dim        int
	watcher    *prophet.Watcher
	stores     map[uint64]*meta.Store
	dbs        map[uint64]*meta.VectorDB
	leaders    map[uint64]uint64
	opts       map[uint64]uint64
	transports map[uint64]*transport
	ctxs       *sync.Map
	requests   *sync.Map
	timeout    time.Duration
	initC      chan struct{}
	maxDB      uint64

	waitC chan *waitReq
	condL *sync.Mutex
	cond  *sync.Cond
}

func newRouter(dim int, timeout time.Duration, addrs ...string) *router {
	lock := &sync.Mutex{}
	return &router{
		dim:        dim,
		watcher:    prophet.NewWatcher(addrs...),
		stores:     make(map[uint64]*meta.Store),
		dbs:        make(map[uint64]*meta.VectorDB),
		leaders:    make(map[uint64]uint64),
		transports: make(map[uint64]*transport),
		ctxs:       &sync.Map{},
		requests:   &sync.Map{},
		timeout:    timeout,
		initC:      make(chan struct{}, 1),
		waitC:      make(chan *waitReq, 1024),
		condL:      lock,
		cond:       sync.NewCond(lock),
	}
}

func (r *router) start() {
	c := r.watcher.Watch(prophet.EventFlagAll)
	for {
		evt, ok := <-c
		if !ok {
			return
		}

		switch evt.Event {
		case prophet.EventInit:
			r.updateAll(evt)
			log.Debugf("after init******************")
			r.initC <- struct{}{}
			r.cond.Broadcast()
		case prophet.EventResourceCreated:
			db := parseDB(evt.Value)
			log.Debugf("event: db %d created", db.ID)
			r.addDB(db, true)
			r.cond.Broadcast()
		case prophet.EventResourceChaned:
			db := parseDB(evt.Value)
			log.Debugf("event: db %d changed", db.ID)
			r.updateDB(db)
		case prophet.EventResourceLeaderChanged:
			db, newLeader := evt.ReadLeaderChangerValue()
			log.Debugf("event: db %d leader changer to peer %d", db, newLeader)
			r.updateLeader(db, newLeader)
		case prophet.EventContainerCreated:
			store := parseStore(evt.Value)
			log.Debugf("event: store %d created", store.ID)
			r.addStore(store, true)
		case prophet.EventContainerChanged:
			store := parseStore(evt.Value)
			log.Debugf("event: store %d changed", store.ID)
			r.updateStore(store)
		}
	}
}

func (r *router) startWaitSent() {
	for {
		req := <-r.waitC
		if req == nil {
			log.Infof("send wait req exit")
			return
		}

		r.waitForNewDB(req.oldReq.DB)
		r.foreach(func(db *meta.VectorDB, max bool) {
			if db.ID > req.oldReq.DB {
				target := &rpc.SearchRequest{}
				*target = *req.oldReq
				target.DB = db.ID
				target.ID = uuid.NewV4().Bytes()
				target.Last = max
				req.before(target)
				r.send(db, target)
			}
		})
		req.after()
	}
}

func (r *router) waitForNewDB(old uint64) {
	r.condL.Lock()
	for {
		if r.getMaxDB() <= old {
			r.cond.Wait()
		} else {
			break
		}
	}
	r.condL.Unlock()
}

func (r *router) do(id uint64, cb func(*meta.VectorDB)) {
	r.RLock()
	if db, ok := r.dbs[id]; ok {
		cb(db)
	}
	r.RUnlock()
}

func (r *router) foreach(cb func(*meta.VectorDB, bool)) {
	r.RLock()
	for id, db := range r.dbs {
		cb(db, id == r.maxDB)
	}
	r.RUnlock()
}

func (r *router) getMaxDB() uint64 {
	r.RLock()
	value := r.maxDB
	r.RUnlock()
	return value
}

func (r *router) updateStore(store *meta.Store) {
	r.Lock()
	if _, ok := r.stores[store.ID]; !ok {
		log.Fatal("bugs: update a not exist store of event notify")
	}
	r.stores[store.ID] = store
	r.Unlock()
}

func (r *router) addStore(store *meta.Store, lock bool) {
	if lock {
		r.Lock()
	}

	if _, ok := r.stores[store.ID]; ok {
		log.Fatal("bugs: add a exist store of event notify")
	}
	r.stores[store.ID] = store
	r.transports[store.ID] = newTransport(store.ClientAddress, r.timeout, r)
	go r.transports[store.ID].start()

	if lock {
		r.Unlock()
	}
}

func (r *router) updateLeader(db, leader uint64) {
	r.Lock()
	if _, ok := r.dbs[db]; !ok {
		log.Fatal("bugs: update leader with a not exist db of event notify")
	}
	r.leaders[db] = leader
	r.Unlock()
}

func (r *router) updateDB(db *meta.VectorDB) {
	r.Lock()
	if _, ok := r.dbs[db.ID]; !ok {
		log.Fatal("bugs: update a not exist db of event notify")
	}
	r.dbs[db.ID] = db
	r.updateMaxDB(db)
	r.Unlock()
}

func (r *router) addDB(db *meta.VectorDB, lock bool) {
	if lock {
		r.Lock()
	}

	if _, ok := r.dbs[db.ID]; ok {
		log.Fatal("bugs: add a exist db of event notify")
	}
	r.dbs[db.ID] = db
	r.updateMaxDB(db)

	if lock {
		r.Unlock()
	}
}

func (r *router) updateMaxDB(db *meta.VectorDB) {
	if db.ID > r.maxDB {
		r.maxDB = db.ID
	}
}

func (r *router) updateAll(evt *prophet.EventNotify) {
	r.Lock()
	r.stores = make(map[uint64]*meta.Store)
	r.dbs = make(map[uint64]*meta.VectorDB)
	r.maxDB = 0

	dbF := func(data []byte, leader uint64) {
		db := parseDB(data)
		r.addDB(db, false)

		if leader > 0 {
			r.leaders[db.ID] = leader
		}
	}

	storeF := func(data []byte) {
		r.addStore(parseStore(data), false)
	}
	evt.ReadInitEventValues(dbF, storeF)
	r.Unlock()
}

func parseDB(data []byte) *meta.VectorDB {
	value := &meta.VectorDB{}
	pbutil.MustUnmarshal(value, data)
	return value
}

func parseStore(data []byte) *meta.Store {
	value := &meta.Store{}
	pbutil.MustUnmarshal(value, data)
	return value
}

type waitReq struct {
	oldReq *rpc.SearchRequest
	before func(*rpc.SearchRequest)
	after  func()
}
