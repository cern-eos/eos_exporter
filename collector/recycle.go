package collector

import (
	"context"
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
	UsedBytes          *prometheus.GaugeVec
	MaxBytes           *prometheus.GaugeVec
	VolumeUsagePercent *prometheus.GaugeVec
	InodeUsagePercent  *prometheus.GaugeVec
	Lifetime           *prometheus.GaugeVec
	Ratio              *prometheus.GaugeVec
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
				Name:        "recyle_usedbytes",
				Help:        "Recycle Used Bytes",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
		MaxBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "recycle_maxbytes",
				Help:        "Recycle Max Bytes",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
		VolumeUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "recycle_volumeusagepercent",
				Help:        "Volume usage (percent)",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
		InodeUsagePercent: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "recycle_inodeusagepercent",
				Help:        "Inode usage (percent)",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
		Lifetime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "recycle_lifetime",
				Help:        "Recycle lifetime",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
		Ratio: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "recycle_ratio",
				Help:        "Space Disk Read Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"recycle-bin"},
		),
	}
}

func (o *RecycleCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.UsedBytes,
		o.MaxBytes,
		o.VolumeUsagePercent,
		o.InodeUsagePercent,
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

	usedbytes, err := strconv.ParseFloat(mds.UsedBytes, 64)
	if err == nil {
		o.UsedBytes.WithLabelValues(mds.RecycleBin).Set(usedbytes)
	}

	maxbytes, err := strconv.ParseFloat(mds.MaxBytes, 64)
	if err == nil {
		o.MaxBytes.WithLabelValues(mds.RecycleBin).Set(maxbytes)
	}

	lifetime, err := strconv.ParseFloat(mds.Lifetime, 64)
	if err == nil {
		o.Lifetime.WithLabelValues(mds.RecycleBin).Set(lifetime)
	}

	ratio, err := strconv.ParseFloat(mds.Ratio, 64)
	if err == nil {
		o.Ratio.WithLabelValues(mds.RecycleBin).Set(ratio)
	}

	return nil

} // collectRecycleDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *RecycleCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
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
