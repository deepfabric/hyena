package raftstore

import (
	"sync"

	"github.com/fagongzi/log"
	pbutil "github.com/fagongzi/util/protoc"
	bizCodc "github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
	"github.com/youzan/go-nsq"
)

func (pr *PeerReplicate) maybeStartConsumer() {
	if pr.consumerRunning {
		return
	}

	pr.consumerRunning = true

	go func() {
		nc := nsq.NewConfig()
		nc.EnableOrdered = true
		nc.MaxAttempts = 1

		c, err := nsq.NewConsumer(pr.store.cfg.Topic, pr.store.cfg.Channel, nc)
		if err != nil {
			log.Fatalf("raftstore[%d]: create nsq consumer failed, errors:\n%+v",
				pr.id,
				err)
		}
		c.AddHandler(pr)
		err = c.ConnectToNSQLookupds(pr.store.cfg.NSQLookupURLs)
		if err != nil {
			log.Fatalf("raftstore[%d]: connect to nsq lookups failed, errors:\n%+v",
				pr.id,
				err)
		}

		pr.consumer = c
		pr.nsqRequests = util.NewRingBuffer(uint64(batch))
		pr.condL = &sync.Mutex{}
		pr.cond = sync.NewCond(pr.condL)
	}()
}

func (pr *PeerReplicate) maybeStopConsumer() {
	if !pr.consumerRunning {
		return
	}

	if pr.consumer != nil {
		pr.consumerRunning = false
		pr.consumer.Stop()
	}
}

// LogFailedMessage nsq handler
func (pr *PeerReplicate) LogFailedMessage(message *nsq.Message) {
}

// HandleMessage nsq handler
func (pr *PeerReplicate) HandleMessage(message *nsq.Message) error {
	buf := acquireBuf(len(message.Body))
	buf.Write(message.Body)
	_, msg, err := bizCodc.GetDecoder().Decode(buf)
	if err != nil {
		log.Fatalf("bug: decode from nsq failed, errors:\n%+v",
			err)
	}

	if req, ok := msg.(*rpc.InsertRequest); ok {
		req.Offset = message.Offset
		pr.nsqRequests.Put(req)
		pr.store.addApplyJob(pr.id, "handle nsq insert", pr.doInsert, nil)
	} else if req, ok := msg.(*rpc.UpdateRequest); ok {
		pr.onUpdate(req, nil, nil)
	}

	return nil
}

func (pr *PeerReplicate) doInsert() error {
	if pr.nsqRequests == nil {
		log.Fatal("bug: nsq request ring is nil")
	}

	committedOffset := uint64(0)
	vBatch := acquireVdbBatch()
	for i := int64(0); i < batch && pr.nsqRequests.Len() > 0; i++ {
		msg, err := pr.nsqRequests.Get()
		if err != nil {
			log.Fatalf("bug: get from nsq request errors:%+v", err)
		}
		req := msg.(*rpc.InsertRequest)
		committedOffset = req.Offset
		vBatch.append(req.Xbs, req.Ids)
		util.ReleaseInsertReq(req)
	}

	err := pr.ps.vdb.AddWithIds(vBatch.xbs, vBatch.ids)
	if err != nil {
		log.Fatalf("raftstore[db-%d]: batch insert failed, errors:%+v",
			pr.id,
			err)
	}

	if committedOffset > 0 {
		pr.ps.setCommittedOffset(committedOffset)
		err := pr.store.metaStore.Set(getRaftApplyStateKey(pr.id), pbutil.MustMarshal(&pr.ps.raftApplyState), false)
		if err != nil {
			log.Fatalf("raftstore[db-%d]: save apply context failed, errors:\n %+v",
				pr.id,
				err)
		}

		pr.cond.Broadcast()
	}

	releaseVdbBatch(vBatch)
	return nil
}
