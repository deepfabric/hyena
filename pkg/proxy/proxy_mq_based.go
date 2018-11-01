package proxy

import (
	"sync"

	"github.com/Shopify/sarama"
	"github.com/fagongzi/goetty"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/rpc"
)

type mqBasedProxy struct {
	sync.RWMutex

	opts            *options
	topic           string
	addrs           []string
	producer        sarama.SyncProducer
	committedOffset int64

	prophetAddrs []string
	router       *router
}

// NewMQBasedProxy return a Proxy based on mq
func NewMQBasedProxy(topic string, addrs []string, prophetAddrs []string, opts ...Option) (Proxy, error) {
	p := new(mqBasedProxy)
	p.topic = topic
	p.addrs = addrs
	p.prophetAddrs = prophetAddrs
	p.opts = &options{}

	for _, opt := range opts {
		opt(p.opts)
	}

	p.opts.adjust()

	err := p.initProducer()
	if err != nil {
		return nil, err
	}

	p.initRouter()
	return p, nil
}

func (p *mqBasedProxy) initRouter() {
	p.router = newRouter(p.opts.dim, p.opts.timeout, p.prophetAddrs...)
	go p.router.start()
	<-p.router.initC
}

func (p *mqBasedProxy) initProducer() error {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Retry.Max = 10
	config.Producer.Return.Successes = true

	producer, err := sarama.NewSyncProducer(p.addrs, config)
	if err != nil {
		return err
	}

	p.producer = producer
	return nil
}

func (p *mqBasedProxy) UpdateWithIds(db uint64, extXb []float32, extXids []int64) error {
	req := &rpc.UpdateRequest{
		DB:  db,
		Xbs: extXb,
		Ids: extXids,
	}

	return p.doPublish(req, req.Size())
}

func (p *mqBasedProxy) AddWithIds(newXb []float32, newXids []int64) error {
	req := &rpc.InsertRequest{
		Xbs: newXb,
		Ids: newXids,
	}

	return p.doPublish(req, req.Size())
}

func (p *mqBasedProxy) Search(xq []float32) (uint64, []float32, []int64, error) {
	req := acquireRequest()
	req.Offset = p.getOffset()
	req.Xq = xq

	db, ds, ids, err := p.router.search(req)
	releaseRequest(req)
	return db, ds, ids, err
}

func (p *mqBasedProxy) doPublish(req interface{}, size int) error {
	buf := goetty.NewByteBuf(size + 5)
	err := codec.GetEncoder().Encode(req, buf)
	if err != nil {
		buf.Release()
		return err
	}

	n, data, err := buf.ReadAll()
	_, offset, err := p.producer.SendMessage(&sarama.ProducerMessage{
		Topic: p.topic,
		Value: sarama.ByteEncoder(data),
	})
	buf.Release()
	if err != nil {
		return err
	}

	p.resetOffset(offset)
	log.Infof("topic %s published %d bytes with offset %d",
		p.topic,
		n,
		p.getOffset())
	return nil
}

func (p *mqBasedProxy) resetOffset(offset int64) {
	p.Lock()
	if p.committedOffset < offset {
		p.committedOffset = offset
	}
	p.Unlock()
}

func (p *mqBasedProxy) getOffset() int64 {
	p.RLock()
	value := p.committedOffset
	p.RUnlock()
	return value
}
