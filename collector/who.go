package collector

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
	//"os"
	//"bufio"
	//"strings"
)

// eos who -m
// uid=fdfdfdf nsessions=1

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
			[]string{"uid"},
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

	mds, err := client.Who(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	for _, m := range mds {
		nsessions, err := strconv.ParseFloat(m.SessionNumber, 64)
		if err == nil {
			o.SessionNumber.WithLabelValues(m.Uid).Set(nsessions)
		}
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
		log.Println("failed collecting recycle metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
