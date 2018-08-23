package raftstore

import (
	"fmt"
	"sync"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/fagongzi/log"
	bizCodc "github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

func (pr *PeerReplicate) maybeStartConsumer() {
	pr.consumerStartOnce.Do(func() {
		go func() {
			config := cluster.NewConfig()
			config.Group.Return.Notifications = true
			config.Consumer.Return.Errors = true

			err := util.PatchSaramaOffset(config, pr.ps.committedOffset())
			if err != nil {
				log.Fatalf("raftstore[%d]: start mq consumer failed: %+v",
					pr.id,
					err)
			}

			c, err := cluster.NewConsumer(pr.store.cfg.MQAddrs, fmt.Sprintf("%s-%d", pr.store.cfg.GroupPrefix, pr.peer.ID), []string{pr.store.cfg.Topic}, config)
			if err != nil {
				log.Fatal(err)
				return
			}

			pr.consumer = c
			pr.condL = &sync.Mutex{}
			pr.cond = sync.NewCond(pr.condL)
			pr.doStartConsumerLoops()
			log.Infof("raftstore[%d]: ********start mq consumer with offset %d********",
				pr.id,
				config.Consumer.Offsets.Initial)
		}()
	})
}

func (pr *PeerReplicate) maybeStopConsumer() {
	if pr.consumer != nil {
		pr.consumerCloseOnce.Do(func() {
			err := pr.consumer.Close()
			if err != nil {
				log.Fatalf("raftstore[%d]: stop mq consumer failed: %+v",
					pr.id,
					err)
			}
			pr.mqRequests.Dispose()
			log.Infof("raftstore[%d]: ********stop mq consumer********",
				pr.id)
		})
	}
}

func (pr *PeerReplicate) doStartConsumerLoops() {
	go func() {
		for {
			err, ok := <-pr.consumer.Errors()
			if !ok {
				log.Errorf("raftstore[%d]: mq consumer error loop exit",
					pr.id)
				return
			}

			log.Errorf("raftstore[%d]: mq consumer fialed: %+v",
				pr.id,
				err)
		}
	}()

	go func() {
		for {
			nty, ok := <-pr.consumer.Notifications()
			if !ok {
				log.Errorf("raftstore[%d]: mq consumer notify loop exit",
					pr.id)
				return
			}

			log.Infof("raftstore[%d]: mq consumer notify: %+v",
				pr.id,
				nty)
		}
	}()

	go func() {
		for {
			message, ok := <-pr.consumer.Messages()
			if !ok {
				log.Errorf("raftstore[db-%d]: mq consumer read loop exit",
					pr.id)
				return
			}

			buf := acquireBuf(len(message.Value))
			buf.Write(message.Value)
			_, msg, err := bizCodc.GetDecoder().Decode(buf)
			if err != nil {
				log.Fatalf("bug: decode from nsq failed, errors:\n%+v",
					err)
			}

			if req, ok := msg.(*rpc.InsertRequest); ok {
				req.Offset = message.Offset
				pr.addRequestFromMQ(req)
			} else if req, ok := msg.(*rpc.UpdateRequest); ok {
				pr.mqUpdateRequests.Put(req)
			}

			pr.consumer.MarkOffset(message, "")
		}
	}()
}
