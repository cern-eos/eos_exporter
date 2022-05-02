package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type IOInfoCollector struct {
	Measurement *prometheus.GaugeVec
	Total       *prometheus.GaugeVec
	Last_60s    *prometheus.GaugeVec
	Last_300s   *prometheus.GaugeVec
	Last_3600s  *prometheus.GaugeVec
	Last_86400s *prometheus.GaugeVec
}

//NewIOInfoCollector creates an cluster of the IOInfoCollector
func NewIOInfoCollector(cluster string) *IOInfoCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &IOInfoCollector{

		Total: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "iostat_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{"iostat", "app"},
		),
	}
}

func (o *IOInfoCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Total,
	}
}

func (o *IOInfoCollector) collectIOInfoDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListIOInfo(context.Background())
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		total, err := strconv.ParseFloat(m.Total, 64)
		if err == nil {
			if m.Application == "NA" {
				o.Total.WithLabelValues(m.Measurement, "not_applicable").Set(total)
			} else {
				o.Total.WithLabelValues(m.Measurement, m.Application).Set(total)
			}
		}

	}

	return nil

} // collectIOInfoDF()

// Describe sends the descriptors of each IOInfoCollector related metrics we have defined
func (o *IOInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *IOInfoCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectIOInfoDF(); err != nil {
		log.Println("failed collecting IO info metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
