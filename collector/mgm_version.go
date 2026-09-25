package collector

import (
	"context"
	"log"
	"sync"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

const (
	mgmLeaderVersionRefreshInterval = 30 * time.Second
	mgmLeaderVersionTimeout         = 5 * time.Second
)

type MGMLeaderVersionCollector struct {
	*CollectorOpts
	mu            sync.Mutex
	lastRefresh   time.Time
	refreshing    bool
	version       *eosclient.MGMLeaderVersion
	failureLogged bool
	fetch         func(context.Context) (*eosclient.MGMLeaderVersion, error)
	desc          *prometheus.Desc
}

func NewMGMLeaderVersionCollector(opts *CollectorOpts) *MGMLeaderVersionCollector {
	o := &MGMLeaderVersionCollector{
		CollectorOpts: opts,
		desc: prometheus.NewDesc(
			"eos_mgm_leader_info",
			"Version and release of this MGM, emitted only while it is the leader.",
			[]string{"mgm", "eos_version", "eos_release"},
			prometheus.Labels{"cluster": opts.Cluster},
		),
	}
	o.fetch = func(ctx context.Context) (*eosclient.MGMLeaderVersion, error) {
		client, err := eosclient.New(&eosclient.Options{
			URL:     "root://localhost",
			Timeout: opts.Timeout,
		})
		if err != nil {
			return nil, err
		}
		return client.GetLocalMGMLeaderVersion(ctx)
	}
	return o
}

func (o *MGMLeaderVersionCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- o.desc
}

func (o *MGMLeaderVersionCollector) Collect(ch chan<- prometheus.Metric) {
	o.mu.Lock()
	if !o.refreshing && time.Since(o.lastRefresh) >= mgmLeaderVersionRefreshInterval {
		o.refreshing = true
		o.lastRefresh = time.Now()
		go o.refresh()
	}
	version := o.version
	o.mu.Unlock()

	if version != nil {
		ch <- prometheus.MustNewConstMetric(o.desc, prometheus.GaugeValue, 1,
			version.Leader, version.Version, version.Release)
	}
}

func (o *MGMLeaderVersionCollector) refresh() {
	ctx, cancel := context.WithTimeout(context.Background(), mgmLeaderVersionTimeout)
	defer cancel()
	version, err := o.fetch(ctx)
	o.mu.Lock()
	if err != nil {
		if !o.failureLogged {
			log.Printf("failed collecting leader MGM version: %v", err)
			o.failureLogged = true
		}
	} else {
		o.failureLogged = false
	}
	o.version = version
	o.refreshing = false
	o.mu.Unlock()
}
