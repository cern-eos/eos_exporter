package collector

import (
	"context"
	"fmt"
	"log"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

// eos who -a -m provides 3 clusters of information
// a) Aggregation of number of sessions by protocol, examples:
// auth=gsi nsessions=2
// auth=https nsessions=3093
// b) Aggregation by uid
// uid=982 nsessions=2
// uid=983 nsessions=2
// c) Client info
// client=yyy@xxx.cern.ch uid=yyy auth=https idle=66 gateway="xxx.cern.ch" app=http
// Because a) and b) can be derived from c), we only report c) in the metric
// Aggregation on fields from c) can be done in the monitoring system
// This collector provides metrics based on c)

type WhoCollector struct {
	SessionNumber *prometheus.GaugeVec
}

//NewWhoCollector creates an cluster of the WhoCollector
func NewWhoCollector(cluster string) *WhoCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	namespace := "eos"

	return &WhoCollector{
		SessionNumber: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "who",
				Help:        "sessions opened",
				ConstLabels: labels,
			},
			[]string{"app", "auth", "gateway", "uid"},
		),
	}
}

func (o *WhoCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.SessionNumber,
	}
}

func (o *WhoCollector) collectWhoDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	whos, err := client.Who(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	// we need to aggregate for the value
	counter := map[string]int{}
	for _, m := range whos {
		if _, ok := counter[m.Serialized]; ok {
			counter[m.Serialized]++
		} else {
			counter[m.Serialized] = 1
		}
	}

	seen := map[string]bool{}
	for _, m := range whos {
		s := m.Serialized
		if _, ok := seen[s]; ok {
			continue
		}

		v := counter[s]
		o.SessionNumber.WithLabelValues(m.App, m.Auth, m.Gateway, m.Uid).Set(float64(v))
		seen[s] = true
	}

	return nil

} // collectWhoDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *WhoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		fmt.Print(metric)
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *WhoCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectWhoDF(); err != nil {
		log.Println("failed collecting who  metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
