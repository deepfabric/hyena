// Copyright 2016 DeepFabric, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/raftstore"
	"github.com/infinivision/hyena/pkg/server"
	"github.com/infinivision/hyena/pkg/util"
	"github.com/infinivision/prophet"
)

const (
	kb      = 1024
	mb      = 1024 * kb
	million = 1000000
)

var (
	dataPath = flag.String("data", "/tmp/hyena", "The data dir")
	addr     = flag.String("addr", "127.0.0.1:9527", "Internal address")
	addrRaft = flag.String("addr-raft", "127.0.0.1:9528", "Raft address")
	pprof    = flag.String("addr-pprof", "", "pprof http server address")

	// about prophet
	prophetName                             = flag.String("prophet-name", "node1", "Prophet: name of this node")
	prophetAddr                             = flag.String("prophet-addr", "127.0.0.1:9529", "Prophet: rpc address")
	prophetNamespace                        = flag.String("prophet-namespace", "/prophet", "Prophet: namespace")
	prophetURLsClient                       = flag.String("prophet-urls-client", "http://127.0.0.1:2371", "Prophet: embed etcd client urls")
	prophetURLsAdvertiseClient              = flag.String("prophet-urls-advertise-client", "", "Prophet: embed etcd client advertise urls")
	prophetURLsPeer                         = flag.String("prophet-urls-peer", "http://127.0.0.1:2381", "Prophet: embed etcd peer urls")
	prophetURLsAdvertisePeer                = flag.String("prophet-urls-advertise-peer", "", "Prophet: embed etcd peer advertise urls")
	prophetInitialCluster                   = flag.String("prophet-initial-cluster", "node1=http://127.0.0.1:2381", "Prophet: embed etcd initial cluster")
	prophetInitialClusterState              = flag.String("prophet-initial-cluster-state", "new", "Prophet: embed etcd initial cluster state")
	prophetLocationLabel                    = flag.String("prophet-location-label", "zone,rack", "Prophet: store location label name")
	prophetLeaderLeaseTTLSec                = flag.Int64("prophet-leader-lease", 5, "Prophet: seconds of leader lease ttl")
	prophetScheduleRetries                  = flag.Int("prophet-schedule-max-retry", 3, "Prophet: max schedule retries times when schedule failed")
	prophetScheduleMaxIntervalSec           = flag.Int("prophet-schedule-max-interval", 60, "Prophet: maximum seconds between twice schedules")
	prophetScheduleMinIntervalMS            = flag.Int("prophet-schedule-min-interval", 10, "Prophet: minimum millisecond between twice schedules")
	prophetTimeoutWaitOperatorCompleteMin   = flag.Int("prophet-timeout-wait-operator", 5, "Prophet: timeout for waitting teh operator complete")
	prophetMaxFreezeScheduleIntervalSec     = flag.Int("prophet-schedule-max-freeze-interval", 30, "Prophet: maximum seconds freeze the container for a while if no need to schedule")
	prophetMaxAllowContainerDownDurationMin = flag.Int("prophet-max-allow-container-down", 60, "Prophet: maximum container down mins, the container will removed from replicas")
	prophetMaxRebalanceLeader               = flag.Uint64("prophet-max-rebalance-leader", 16, "Prophet: maximum count of transfer leader operator")
	prophetMaxRebalanceReplica              = flag.Uint64("prophet-max-rebalance-replica", 12, "Prophet: maximum count of remove|add replica operator")
	prophetMaxScheduleReplica               = flag.Uint64("prophet-schedule-max-replica", 12, "Prophet: maximum count of schedule replica operator")
	prophetMaxLimitSnapshotsCount           = flag.Uint64("prophet-max-snapshot", 3, "Prophet: maximum count of node about snapshot with schedule")
	prophetCountResourceReplicas            = flag.Int("prophet-resource-replica", 3, "Prophet: replica number per resource")
	prophetMinAvailableStorageUsedRate      = flag.Int("prophet-min-storage", 80, "Prophet: minimum storage used rate of container, if the rate is over this value, skip the container")
	prophetMaxRPCConns                      = flag.Int("prophet-rpc-conns", 10, "Prophet: maximum connections for rpc")
	prophetRPCConnIdleSec                   = flag.Int("prophet-rpc-idle", 60*60, "Prophet(Sec): maximum idle time for rpc connection")
	prophetRPCTimeoutSec                    = flag.Int("prophet-rpc-timeout", 10, "Prophet(Sec): maximum timeout to wait rpc response")
	prophetStorageNode                      = flag.Bool("prophet-storage", true, "Prophet: is storage node, if true enable embed etcd server")

	// about raftstore
	optionPath                = flag.String("raft-store-option", "", "The store option file")
	zone                      = flag.String("zone", "zone-1", "Zone label")
	rack                      = flag.String("rack", "rack-1", "Rack label")
	raftTickDurationMS        = flag.Int("raft-tick", 1000, "Raft(ms): Raft tick")
	electionTick              = flag.Int("raft-election-tick", 10, "Raft: leader election, after this ticks")
	heartbeatTick             = flag.Int("raft-heartbeat-tick", 2, "Raft: heartbeat, after this ticks")
	maxSizePerMsgMB           = flag.Uint64("raft-max-msg-bytes", 1, "Raft(MB): Max bytes per raft msg")
	maxRaftEntryBytesMB       = flag.Uint64("raft-max-entry-bytes", 8, "Raft(MB): Max bytes of raft log entry")
	maxInflightMsgs           = flag.Int("raft-max-inflight", 256, "Raft: Max count of in-flight raft append messages")
	minRaftLogCount           = flag.Uint64("raft-min-log", 64, "Raft: minimum raft Log count to compact, count of [first, replicated]")
	maxRaftLogCount           = flag.Uint64("raft-max-log", 1024, "Raft: count of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]")
	maxRaftLogBytesMB         = flag.Uint64("raft-max-log-bytes", 32, "Raft(MB): total bytes of raft logs, if reach this limit, leader will compact [first,applied], otherwise [first, minimum replicated]")
	maxRaftLogLag             = flag.Uint64("raft-max-lag", 0, "Raft: max count of lag log, leader will compact [first, compact - lag], avoid send snapshot file to a little lag peer")
	raftLogCompactDurationSec = flag.Int("raft-compact", 10, "Raft(sec): compact raft log")
	syncWrite                 = flag.Bool("raft-sync", false, "Raft: sync to disk while append the raft log")
	maxBatchingSize           = flag.Int("raft-batch-proposal", 1024, "Raft: max commands in a proposal.")
	maxDBRecordsMillion       = flag.Uint64("raft-db-capacity", 1, "Raft(Million): max records per vectordb ")
	sentRaftWorkerCount       = flag.Uint64("raft-worker-raft", 8, "Raft: workers for sent raft messages")
	sentSnapWorkerCount       = flag.Uint64("raft-worker-snap", 4, "Raft: workers for sent snap messages")
	applyWorkerCount          = flag.Uint64("raft-worker-apply", 1, "Raft: workers for apply raft log")
	limitSnapChunkRate        = flag.Int("raft-limit-snap-chunk-rate", 16, "Raft: snap chunks sent per second")
	limitSnapChunkBytesMB     = flag.Uint64("raft-limit-snap-chunk-bytes", 4, "Raft(MB): max snap chunk size")
	storeHBIntervalSec        = flag.Int("raft-heartbeat-store", 30, "Raft(sec): store heartbeat")
	dbHBIntervalSec           = flag.Int("raft-heartbeat-db", 10, "Raft(sec): db heartbeat")
	maxPeerDownTimeSec        = flag.Int("raft-max-peer-down", 5*60, "Raft(sec): Max peer downtime")

	// about vectordb
	dim     = flag.Int("dim", 512, "vector dim")
	flatThr = flag.Int("flat", 1000, "vector flatThr")
	distThr = flag.Float64("dist", 0.6, "vector distThr")

	// about nsq
	topic       = flag.String("mq-topic", "", "MQ: topic")
	groupPrefix = flag.String("mq-group", "hyena-", "MQ: group prefix")
	mqAddr      = flag.String("mq-addr", "", "MQ: mq addrs")

	// about version
	version = flag.Bool("version", false, "show version")
)

func main() {
	flag.Parse()
	log.InitLog()

	if *version {
		util.PrintVersion()
		os.Exit(0)
	}

	if "" != *pprof {
		log.Infof("start pprof at: %s", *pprof)
		go func() {
			log.Fatalf("start pprof failed, errors:\n%+v",
				http.ListenAndServe(*pprof, nil))
		}()
	}
	prophet.SetLogger(&adapterLog{})

	s := server.NewServer(*addr, *addrRaft, parseOptions()...)
	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go s.Start()
	for {
		sig := <-sc
		retVal := 0
		if sig != syscall.SIGTERM {
			retVal = 1
		}
		log.Infof("exit: signal=<%d>.", sig)
		s.Stop()
		log.Infof("exit: bye :-).")
		os.Exit(retVal)
	}
}

func parseOptions() []server.Option {
	var opts []server.Option

	if *dataPath == "" {
		fmt.Println("data path name must be set")
		os.Exit(-1)
	}

	if *prophetName == "" {
		fmt.Println("prophet name must be set")
		os.Exit(-1)
	}

	if *prophetInitialCluster == "" {
		fmt.Println("prophet initial cluster must be set")
		os.Exit(-1)
	}

	if *zone == "" {
		fmt.Println("zone must be set")
		os.Exit(-1)
	}

	if *rack == "" {
		fmt.Println("rack must be set")
		os.Exit(-1)
	}

	if *topic == "" {
		fmt.Println("mq topic must be set")
		os.Exit(-1)
	}

	if *groupPrefix == "" {
		fmt.Println("mq group prefix must be set")
		os.Exit(-1)
	}

	if *mqAddr == "" {
		fmt.Println("mq address urls must be set")
		os.Exit(-1)
	}

	// prophet
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetName(*prophetName)))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetAddr(*prophetAddr)))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithNamespace(*prophetNamespace))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithLocationLabels(strings.Split(*prophetLocationLabel, ",")))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithLeaseTTL(*prophetLeaderLeaseTTLSec))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxScheduleRetries(*prophetScheduleRetries))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxScheduleInterval(time.Second*time.Duration(*prophetScheduleRetries)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMinScheduleInterval(time.Millisecond*time.Duration(*prophetScheduleMinIntervalMS)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithTimeoutWaitOperatorComplete(time.Minute*time.Duration(*prophetTimeoutWaitOperatorCompleteMin)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxFreezeScheduleInterval(time.Second*time.Duration(*prophetMaxFreezeScheduleIntervalSec)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxAllowContainerDownDuration(time.Minute*time.Duration(*prophetMaxAllowContainerDownDurationMin)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxRebalanceLeader(*prophetMaxRebalanceLeader))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxRebalanceReplica(*prophetMaxRebalanceReplica))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxScheduleReplica(*prophetMaxScheduleReplica))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxLimitSnapshotsCount(*prophetMaxLimitSnapshotsCount))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithCountResourceReplicas(*prophetCountResourceReplicas))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMinAvailableStorageUsedRate(*prophetMinAvailableStorageUsedRate))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxRPCCons(*prophetMaxRPCConns))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxRPCConnIdle(time.Second*time.Duration(*prophetRPCConnIdleSec)))))
	opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithMaxRPCTimeout(time.Second*time.Duration(*prophetRPCTimeoutSec)))))

	if *prophetStorageNode {
		embedEtcdCfg := &prophet.EmbeddedEtcdCfg{}
		embedEtcdCfg.DataPath = fmt.Sprintf("%s/prophet", *dataPath)
		embedEtcdCfg.InitialCluster = *prophetInitialCluster
		embedEtcdCfg.InitialClusterState = *prophetInitialClusterState
		embedEtcdCfg.Name = *prophetName
		embedEtcdCfg.URLsAdvertiseClient = *prophetURLsAdvertiseClient
		embedEtcdCfg.URLsAdvertisePeer = *prophetURLsAdvertisePeer
		embedEtcdCfg.URLsClient = *prophetURLsClient
		embedEtcdCfg.URLsPeer = *prophetURLsPeer
		opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithEmbeddedEtcd(embedEtcdCfg))))
	} else {
		var endpoints []string
		if *prophetURLsAdvertiseClient != "" {
			endpoints = strings.Split(*prophetURLsAdvertiseClient, ",")
		} else if *prophetURLsClient != "" {
			endpoints = strings.Split(*prophetURLsClient, ",")
		} else {
			fmt.Println("if is not storage prophet node, prophet-urls-client or prophet-urls-advertise-client must be set")
			os.Exit(-1)
		}

		client, err := clientv3.New(clientv3.Config{
			Endpoints:   endpoints,
			DialTimeout: prophet.DefaultTimeout,
		})
		if err != nil {
			fmt.Printf("init etcd client failed: %+v\n", err)
			os.Exit(-1)
		}

		opts = append(opts, server.WithRaftOption(raftstore.WithProphetOption(prophet.WithExternalEtcd(client))))
	}

	// raftstore
	if *optionPath != "" {
		opts = append(opts, server.WithRaftOption(raftstore.WithStoreOptionsPath(*optionPath)))
	}
	opts = append(opts, server.WithRaftOption(raftstore.WithStoreDataPath(fmt.Sprintf("%s/raftstore", *dataPath))))
	opts = append(opts, server.WithRaftOption(raftstore.WithLabel("zone", *zone)))
	opts = append(opts, server.WithRaftOption(raftstore.WithLabel("rack", *rack)))
	opts = append(opts, server.WithRaftOption(raftstore.WithRaftTickDuration(time.Millisecond*time.Duration(*raftTickDurationMS))))
	opts = append(opts, server.WithRaftOption(raftstore.WithElectionTick(*electionTick)))
	opts = append(opts, server.WithRaftOption(raftstore.WithHeartbeatTick(*heartbeatTick)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxSizePerMsg(*maxSizePerMsgMB*mb)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxRaftEntryBytes(*maxRaftEntryBytesMB*mb)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxInflightMsgs(*maxInflightMsgs)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMinRaftLogCount(*minRaftLogCount)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxRaftLogCount(*maxRaftLogCount)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxRaftLogBytes(*maxRaftLogBytesMB*mb)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxRaftLogLag(*maxRaftLogLag)))
	opts = append(opts, server.WithRaftOption(raftstore.WithRaftLogCompactDuration(time.Second*time.Duration(*raftLogCompactDurationSec))))
	opts = append(opts, server.WithRaftOption(raftstore.WithSyncWrite(*syncWrite)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxBatchingSize(*maxBatchingSize)))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxDBRecords(*maxDBRecordsMillion*million)))
	opts = append(opts, server.WithRaftOption(raftstore.WithSentRaftWorkerCount(*sentRaftWorkerCount)))
	opts = append(opts, server.WithRaftOption(raftstore.WithSentSnapWorkerCount(*sentSnapWorkerCount)))
	opts = append(opts, server.WithRaftOption(raftstore.WithApplyWorkerCount(*applyWorkerCount)))
	opts = append(opts, server.WithRaftOption(raftstore.WithLimitSnapChunkRate(*limitSnapChunkRate)))
	opts = append(opts, server.WithRaftOption(raftstore.WithLimitSnapChunkBytes(*limitSnapChunkBytesMB*mb)))
	opts = append(opts, server.WithRaftOption(raftstore.WithStoreHBInterval(time.Second*time.Duration(*storeHBIntervalSec))))
	opts = append(opts, server.WithRaftOption(raftstore.WithDBHBInterval(time.Second*time.Duration(*dbHBIntervalSec))))
	opts = append(opts, server.WithRaftOption(raftstore.WithMaxPeerDownTime(time.Second*time.Duration(*maxPeerDownTimeSec))))
	opts = append(opts, server.WithRaftOption(raftstore.WithDim(*dim)))
	opts = append(opts, server.WithRaftOption(raftstore.WithFlatThr(*flatThr)))
	opts = append(opts, server.WithRaftOption(raftstore.WithDistThr(float32(*distThr))))
	opts = append(opts, server.WithRaftOption(raftstore.WithMQ(*topic, *groupPrefix, strings.Split(*mqAddr, ","))))
	return opts
}

type adapterLog struct{}

func (l *adapterLog) Info(v ...interface{}) {
	log.Info(v...)
}

func (l *adapterLog) Infof(format string, v ...interface{}) {
	log.Infof(format, v...)
}

func (l *adapterLog) Debug(v ...interface{}) {
	log.Debug(v...)
}

func (l *adapterLog) Debugf(format string, v ...interface{}) {
	log.Debugf(format, v...)
}

func (l *adapterLog) Warn(v ...interface{}) {
	log.Warn(v...)
}

func (l *adapterLog) Warnf(format string, v ...interface{}) {
	log.Warnf(format, v...)
}

func (l *adapterLog) Error(v ...interface{}) {}

func (l *adapterLog) Errorf(format string, v ...interface{}) {
	log.Errorf(format, v...)
}

func (l *adapterLog) Fatal(v ...interface{}) {
	log.Fatal(v...)
}

func (l *adapterLog) Fatalf(format string, v ...interface{}) {
	log.Fatalf(format, v...)
}
