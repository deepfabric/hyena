package raftstore

import (
	"fmt"

	cluster "github.com/bsm/sarama-cluster"
	"github.com/fagongzi/log"
	bizCodc "github.com/infinivision/hyena/pkg/codec"
	"github.com/infinivision/hyena/pkg/pb/rpc"
	"github.com/infinivision/hyena/pkg/util"
)

const (
	indexComplete = -1
)

func (pr *PeerReplicate) maybeStartConsumer(from string) {
	pr.consumerStartOnce.Do(func() {
		go func() {
			config := cluster.NewConfig()
			config.Group.Return.Notifications = true
			config.Consumer.Return.Errors = true

			offset, index := pr.ps.committedOffset()
			err := util.PatchSaramaOffset(config, offset, index)
			if err != nil {
				log.Fatalf("raftstore[db-%d]: start mq consumer failed: %+v",
					pr.id,
					err)
			}
			c, err := cluster.NewConsumer(pr.store.cfg.MQAddrs, fmt.Sprintf("%s-%d", pr.store.cfg.GroupPrefix, pr.peer.ID), []string{pr.store.cfg.Topic}, config)
			if err != nil {
				log.Fatal(err)
				return
			}

			pr.consumer = c
			pr.doStartConsumerLoops(config.Consumer.Offsets.Initial)
			log.Infof("raftstore[db-%d]: ********start mq consumer by %s with offset %d********",
				pr.id,
				from,
				config.Consumer.Offsets.Initial)

			pr.cond.Broadcast()
		}()
	})
}

func (pr *PeerReplicate) maybeStopConsumer() {
	if pr.consumer != nil {
		pr.consumerCloseOnce.Do(func() {
			err := pr.consumer.Close()
			if err != nil {
				log.Fatalf("raftstore[db-%d]: stop mq consumer failed: %+v",
					pr.id,
					err)
			}
			pr.mqRequests.Dispose()
			log.Infof("raftstore[db-%d]: ********stop mq consumer********",
				pr.id)
		})
	}
}

func (pr *PeerReplicate) doStartConsumerLoops(offset int64) {
	go func() {
		for {
			err, ok := <-pr.consumer.Errors()
			if !ok {
				log.Infof("raftstore[db-%d]: mq consumer error loop exit",
					pr.id)
				return
			}

			log.Errorf("raftstore[db-%d]: mq consumer fialed: %+v",
				pr.id,
				err)
		}
	}()

	go func() {
		for {
			nty, ok := <-pr.consumer.Notifications()
			if !ok {
				log.Errorf("raftstore[db-%d]: mq consumer notify loop exit",
					pr.id)
				return
			}

			log.Infof("raftstore[db-%d]: mq consumer notify: %+v",
				pr.id,
				nty)
		}
	}()

	go func() {
		buf := acquireBuf(pr.store.cfg.Dim*4 + 256)
		defer releaseBuf(buf)

		for {
			message, ok := <-pr.consumer.Messages()
			if !ok {
				log.Infof("raftstore[db-%d]: mq consumer read loop exit",
					pr.id)
				return
			}

			if offset <= message.Offset {
				buf.Write(message.Value)
				_, msg, err := bizCodc.GetDecoder().Decode(buf)
				buf.Clear()
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
			}

			pr.consumer.MarkOffset(message, "")
		}
	}()
}
