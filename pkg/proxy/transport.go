package proxy

import (
	"sync/atomic"
	"time"

	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/util"
)

const (
	batch = 32
)

type transport struct {
	addr    string
	conn    goetty.IOSession
	queue   *util.Queue
	timeout time.Duration
	state   uint64
	router  *router
}

func newTransport(addr string, timeout time.Duration, router *router) *transport {
	return &transport{
		addr:    addr,
		queue:   util.New(0),
		timeout: timeout,
		router:  router,
	}
}

func (t *transport) start() {
	t.resetConn()
	go t.loop()
}

func (t *transport) stop() {
	atomic.StoreUint64(&t.state, 1)
	t.queue.Dispose()
}

func (t *transport) sent(req interface{}) error {
	return t.queue.Put(req)
}

func (t *transport) stopped() bool {
	return atomic.LoadUint64(&t.state) == 1
}

func (t *transport) call(items []interface{}, n int64) error {
	for i := int64(0); i < n; i++ {
		log.Debugf("%+v sent", items[i])
		t.conn.Write(items[i])
	}

	err := t.conn.Flush()
	if err != nil {
		log.Debugf("search sent failed: %+v", err)
		return err
	}

	rsps := make([]interface{}, n, n)
	for i := int64(0); i < n; i++ {
		rsp, err := t.conn.Read()
		if err != nil {
			return err
		}
		rsps[i] = rsp
	}

	for _, rsp := range rsps {
		t.router.onResponse(rsp)
	}
	return nil
}

func (t *transport) loop() {
	defer func() {
		if err := recover(); err != nil {
			log.Errorf("transport to %s crashed %+v",
				t.addr,
				err)
		}
	}()

	items := make([]interface{}, batch, batch)

OUTER:
	for {
		n, err := t.queue.Get(batch, items)
		if err != nil || t.stopped() {
			t.conn.Close()
			log.Infof("transport: store %s exit write loop", t.addr)
			return
		}

		for {
			err := t.call(items, n)
			if err == nil || t.stopped() {
				continue OUTER
			}

			log.Infof("transport: %d request sent to store %s failed, %+v, retry",
				n,
				t.addr,
				err)
			t.resetConn()
		}
	}
}

func (t *transport) resetConn() {
	if t.conn != nil {
		t.conn.Close()
	}

	t.conn = goetty.NewConnector(t.addr,
		goetty.WithClientEncoder(codec.GetEncoder()),
		goetty.WithClientDecoder(codec.GetDecoder()))
	for {
		_, err := t.conn.Connect()
		if err == nil {
			break
		}

		log.Errorf("transport: connect to store %s failed, errors:\n%+v",
			t.addr,
			err)
		time.Sleep(time.Second * 2)
	}

	log.Infof("transport: connect to %s succeed",
		t.addr)
}
