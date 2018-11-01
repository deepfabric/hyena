package main

import (
	"flag"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/fagongzi/log"
	"github.com/infinivision/hyena/pkg/proxy"
	"github.com/infinivision/prophet"
)

var (
	con           = flag.Int64("c", 0, "The clients.")
	cn            = flag.Int64("cn", 64, "The concurrency per client.")
	dim           = flag.Int64("dim", 128, "dim")
	num           = flag.Int64("n", 0, "The total number.")
	timeout       = flag.Int("timeout", 200, "timeout ms")
	addrs         = flag.String("addrs", "172.19.0.107:9092", "mq addr.")
	prophetsAddrs = flag.String("addrs-prophet", "172.19.0.101:9529,172.19.0.103:9529,172.19.0.104:9529", "The prophet address.")
)

func main() {
	flag.Parse()

	log.InitLog()
	prophet.SetLogger(&adapterLog{})

	gCount := *con
	total := *num
	if total < 0 {
		total = 0
	}

	ready := make(chan struct{}, gCount)
	complate := &sync.WaitGroup{}
	wg := &sync.WaitGroup{}

	countPerG := total / gCount

	ans := newAnalysis()

	var index int64
	mqs := strings.Split(*addrs, ",")
	prophets := strings.Split(*prophetsAddrs, ",")
	vdb, err := proxy.NewMQBasedProxy("hyena", mqs, prophets, proxy.WithSearchTimeout(time.Duration(*timeout)*time.Millisecond))
	if err != nil {
		log.Fatalf("new proxy: %+v", err)
		return
	}

	for index = 0; index < gCount; index++ {
		start := index * countPerG
		end := (index + 1) * countPerG
		if index == gCount-1 {
			end = total
		}

		wg.Add(1)
		complate.Add(1)
		go startG(end-start, wg, complate, ready, ans, vdb)
	}

	wg.Wait()

	ans.start()

	for index = 0; index < gCount; index++ {
		ready <- struct{}{}
	}

	go func() {
		for {
			ans.print()
			time.Sleep(time.Second * 1)
		}
	}()

	complate.Wait()
	ans.print()
}

func startG(total int64, wg, complate *sync.WaitGroup, ready chan struct{}, ans *analysis, vdb proxy.Proxy) {
	if total <= 0 {
		total = math.MaxInt64
	}

	wg.Done()
	<-ready

	n := (*cn) * (*dim)
	xq := make([]float32, n, n)
	start := time.Now()
	for index := int64(0); index < total; index += *cn {
		for k := int64(0); k < n; k++ {
			xq[k] = float32(total) / float32(k+1)
		}

		s := time.Now()
		ans.incrSent(*cn)
		_, _, _, err := vdb.Search(xq)
		if err != nil {
			log.Fatalf("Search failed, %+v", err)
		}
		ans.incrRecv(*cn, time.Now().Sub(s).Nanoseconds())
	}

	end := time.Now()
	fmt.Printf("%s sent %d reqs\n", end.Sub(start), total)
}

type analysis struct {
	sync.RWMutex
	startAt                            time.Time
	recv, sent, prevRecv               int64
	avgLatency, maxLatency, minLatency int64
	totalCost, prevCost                int64
}

func newAnalysis() *analysis {
	return &analysis{}
}

func (a *analysis) setLatency(latency int64) {
	if a.minLatency == 0 || a.minLatency > latency {
		a.minLatency = latency
	}

	if a.maxLatency == 0 || a.maxLatency < latency {
		a.maxLatency = latency
	}

	a.totalCost += latency
	a.avgLatency = a.totalCost / a.recv
}

func (a *analysis) reset() {
	a.Lock()
	a.maxLatency = 0
	a.minLatency = 0
	a.Unlock()
}

func (a *analysis) start() {
	a.startAt = time.Now()
}

func (a *analysis) incrRecv(n, latency int64) {
	a.Lock()
	a.recv += n
	a.setLatency(latency)
	a.Unlock()
}

func (a *analysis) incrSent(n int64) {
	a.Lock()
	a.sent += n
	a.Unlock()
}

func (a *analysis) print() {
	a.Lock()
	fmt.Printf("[%d, %d, %d](%d s), tps: <%d>/s, avg: %s, min: %s, max: %s \n",
		a.sent,
		a.recv,
		(a.sent - a.recv),
		int(time.Now().Sub(a.startAt).Seconds()),
		(a.recv - a.prevRecv),
		time.Duration(a.avgLatency),
		time.Duration(a.minLatency),
		time.Duration(a.maxLatency))
	a.prevRecv = a.recv
	a.prevCost = a.totalCost
	a.Unlock()
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
