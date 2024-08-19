package collector

import (
	"context"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/cern-eos/eos_exporter/eosclient"
)

// sample line
/*
client=eosxd
host=hostname
version=4.8.28
state=online
time="Fri, 25 Mar 2022 15:31:45 GMT"
tof=4.22
delta=0.18
uuid=ad7a6d7a-ac50-11ec-8090-fa163e4b8280
pid=6271
caps=0
fds=0
type=static
mount="/eos/home-i00/"
app=
ino=1
ino-to-del=0
ino-backlog=0
ino-ever=1
ino-ever-del=0
threads=26
total-ram-gb=14.989
free-ram-gb=3.933
vsize-gb=0.417
rsize-gb=0.021
wr-buf-mb=0
ra-buf-mb=0
load1=0.19
leasetime=300
open-files=0
logfile-size=15332
rbytes=0
wbytes=0
n-op=0
rd60-rate-mb=0.00
wr60-rate-mb=0.00
iops60=0.00
xoff=0
ra-xoff=0
ra-nobuf=0
wr-nobuf=0
idle=4
recovery-ok=0
recovery-fail=0
blockedms=0.000000
blockedfunc=none
*/

type FusexCollector struct {
	*CollectorOpts
	Info *prometheus.GaugeVec
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewFusexCollector(opts *CollectorOpts) *FusexCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &FusexCollector{
		CollectorOpts: opts,
		Info: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fusex_info",
				Help:        "fusex mount information",
				ConstLabels: labels,
			},
			[]string{"host", "version"},
		),
	}
}

func (o *FusexCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Info,
	}
}

func (o *FusexCollector) collectFusexDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListFusex(context.Background(), "root")
	if err != nil {
		return err
	}

	o.Info.Reset()

	for _, m := range mds {
		// We just send a dummy 1 as value
		o.Info.WithLabelValues(m.Host, m.Version).Set(1)
	}

	return nil

} // collectFusexDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *FusexCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *FusexCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectFusexDF(); err != nil {
		log.Println("failed collecting fsck metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
