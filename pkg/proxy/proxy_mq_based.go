package proxy

import (
	"hash/crc32"
	"sync"

	"github.com/fagongzi/goetty"
	"github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/youzan/go-nsq"
)

type mqBasedProxy struct {
	sync.RWMutex

	opts            *options
	topic           string
	lookups         []string
	producer        *nsq.TopicProducerMgr
	committedOffset uint64

	prophetAddrs []string
	router       *router
}

// NewMQBasedProxy return a Proxy based on mq
func NewMQBasedProxy(topic string, lookups []string, prophetAddrs []string, opts ...Option) (Proxy, error) {
	p := new(mqBasedProxy)
	p.topic = topic
	p.lookups = lookups
	p.prophetAddrs = prophetAddrs
	p.opts = &options{}

	for _, opt := range opts {
		opt(p.opts)
	}

	err := p.initProducer()
	if err != nil {
		return nil, err
	}

	p.initRouter()
	return p, nil
}

func (p *mqBasedProxy) initRouter() {
	p.router = newRouter(p.opts.timeout, p.prophetAddrs...)
	go p.router.start()
}

func (p *mqBasedProxy) initProducer() error {
	cfg := nsq.NewConfig()
	cfg.EnableTrace = true
	cfg.Hasher = crc32.NewIEEE()

	pm, err := nsq.NewTopicProducerMgr([]string{p.topic}, cfg)
	if err != nil {
		return err
	}

	pm.AddLookupdNodes(p.lookups)
	p.producer = pm
	return nil
}

func (p *mqBasedProxy) UpdateWithIds(db uint64, extXb []float32, extXids []int64) error {
	req := rpc.UpdateRequest{
		DB:  db,
		Xbs: extXb,
		Ids: extXids,
	}

	return p.doPublish(req, req.Size())
}

func (p *mqBasedProxy) AddWithIds(newXb []float32, newXids []int64) error {
	req := rpc.InsertRequest{
		Xbs: newXb,
		Ids: newXids,
	}

	return p.doPublish(req, req.Size())
}

func (p *mqBasedProxy) Search(xq []float32) ([]float32, []int64, error) {
	req := acquireRequest()
	req.Offset = p.getOffset()
	req.Xq = xq

	ds, ids, err := p.router.search(req)
	releaseRequest(req)
	return ds, ids, err
}

func (p *mqBasedProxy) doPublish(req interface{}, size int) error {
	buf := goetty.NewByteBuf(size + 5)
	err := codec.GetEncoder().Encode(req, buf)
	if err != nil {
		buf.Release()
		return err
	}

	data := buf.RawBuf()[:buf.Readable()]
	_, offset, _, err := p.producer.PublishOrdered(p.topic, data, data)
	buf.Release()
	if err != nil {
		return err
	}

	p.resetOffset(offset)
	return nil
}

func (p *mqBasedProxy) resetOffset(offset uint64) {
	p.Lock()
	if p.committedOffset < offset {
		p.committedOffset = offset
	}
	p.Unlock()
}

func (p *mqBasedProxy) getOffset() uint64 {
	p.RLock()
	value := p.committedOffset
	p.RUnlock()
	return value
}
