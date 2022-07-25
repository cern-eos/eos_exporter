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

// eos recycle -m
// recycle-bin=/eos/homecanary/proc/recycle/ usedbytes=365327522885 maxbytes=100000000000000 volumeusage=0.37% inodeusage=1.19% lifetime=15552000 ratio=0.800000

type RecycleCollector struct {
	UsedBytes *prometheus.GaugeVec
	MaxBytes  *prometheus.GaugeVec
	Lifetime  *prometheus.GaugeVec
	Ratio     *prometheus.GaugeVec
}

//NewRecycleCollector creates an cluster of the RecycleCollector
func NewRecycleCollector(cluster string) *RecycleCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &RecycleCollector{
		UsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "recyle_used_bytes",
				Help:        "Recycle Used Bytes",
				ConstLabels: labels,
			},
			[]string{},
		),
		MaxBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "recycle_max_bytes",
				Help:        "Recycle Max Bytes",
				ConstLabels: labels,
			},
			[]string{},
		),
		Lifetime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "recycle_lifetime_seconds",
				Help:        "Recycle purges files older than this",
				ConstLabels: labels,
			},
			[]string{},
		),
		Ratio: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "recycle_ratio",
				Help:        "Recycle purge kicks in above the fill rate",
				ConstLabels: labels,
			},
			[]string{},
		),
	}
}

func (o *RecycleCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.UsedBytes,
		o.MaxBytes,
		o.Lifetime,
		o.Ratio,
	}
}

func (o *RecycleCollector) collectRecycleDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.Recycle(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	for _, m := range mds {
		usedbytes, err := strconv.ParseFloat(m.UsedBytes, 64)
		if err == nil {
			o.UsedBytes.WithLabelValues().Set(usedbytes)
		}

		maxbytes, err := strconv.ParseFloat(m.MaxBytes, 64)
		if err == nil {
			o.MaxBytes.WithLabelValues().Set(maxbytes)
		}

		lifetime, err := strconv.ParseFloat(m.Lifetime, 64)
		if err == nil {
			o.Lifetime.WithLabelValues().Set(lifetime)
		}

		ratio, err := strconv.ParseFloat(m.Ratio, 64)
		if err == nil {
			o.Ratio.WithLabelValues().Set(ratio)
		}
	}

	return nil

} // collectRecycleDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *RecycleCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		fmt.Print(metric)
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *RecycleCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectRecycleDF(); err != nil {
		log.Println("failed collecting recycle metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
