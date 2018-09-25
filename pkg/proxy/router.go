package proxy

import (
	"sync"
	"time"

	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	"github.com/infinivision/hyena/pkg/pb/meta"
	"github.com/infinivision/prophet"
)

type router struct {
	sync.RWMutex

	watcher     *prophet.Watcher
	writeableDB *meta.VectorDB
	stores      map[uint64]*meta.Store
	dbs         map[uint64]*meta.VectorDB
	leaders     map[uint64]uint64
	opts        map[uint64]uint64
	transports  map[uint64]*transport
	ctxs        *sync.Map
	timeout     time.Duration
	initC       chan struct{}
}

func newRouter(timeout time.Duration, addrs ...string) *router {
	return &router{
		watcher:     prophet.NewWatcher(addrs...),
		writeableDB: nil,
		stores:      make(map[uint64]*meta.Store),
		dbs:         make(map[uint64]*meta.VectorDB),
		leaders:     make(map[uint64]uint64),
		transports:  make(map[uint64]*transport),
		ctxs:        &sync.Map{},
		timeout:     timeout,
		initC:       make(chan struct{}, 1),
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
		case prophet.EventResourceCreated:
			db := parseDB(evt.Value)
			log.Debugf("event: db %d created", db.ID)
			r.addDB(db, true)
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
	r.updateWriteable(db)
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
	r.updateWriteable(db)

	if lock {
		r.Unlock()
	}
}

func (r *router) updateWriteable(db *meta.VectorDB) {
	if db.State == meta.RWU {
		r.writeableDB = db
	}
}

func (r *router) updateAll(evt *prophet.EventNotify) {
	r.Lock()
	r.writeableDB = nil
	r.stores = make(map[uint64]*meta.Store)
	r.dbs = make(map[uint64]*meta.VectorDB)
	r.leaders = make(map[uint64]uint64)

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
