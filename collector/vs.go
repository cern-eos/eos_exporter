package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
	//"os"
	//"bufio"
	//"fmt"
	//"strings"
)

type VSCollector struct {
	EOSmgm    *prometheus.GaugeVec
	Hostport  *prometheus.GaugeVec
	Geotag    *prometheus.GaugeVec
	Vsize     *prometheus.GaugeVec
	Rss       *prometheus.GaugeVec
	Threads   *prometheus.GaugeVec
	Versions   *prometheus.GaugeVec
	EOSfst    *prometheus.GaugeVec
	Xrootdfst *prometheus.GaugeVec
	KernelV   *prometheus.GaugeVec
	Start     *prometheus.GaugeVec
	Uptime    *prometheus.GaugeVec
}

//NewFSCollector creates an instance of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewVSCollector(cluster string) *VSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &VSCollector{
		Vsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_vsize_bytes",
				Help:        "Vsize: ",
				ConstLabels: labels,
			},
			[]string{"mgm_version", "node", "geotag", "eos_v_fst", "xrd_v_fst", "kernel_v"},
		),
		Rss: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_rss_bytes",
				Help:        "Rss: ",
				ConstLabels: labels,
			},
			[]string{"mgm_version", "node", "geotag", "eos_v_fst", "xrd_v_fst", "kernel_v"},
		),
		Threads: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_threads_total",
				Help:        "Threads: ",
				ConstLabels: labels,
			},
			[]string{"mgm_version", "node", "geotag", "eos_v_fst", "xrd_v_fst", "kernel_v"},
		),
		Versions: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_total",
				Help:        "Verions: Amount of daemons attached to a node",
				ConstLabels: labels,
			},
			[]string{"mgm_version", "node", "port", "geotag", "eos_v_fst", "xrd_v_fst", "kernel_v"},
		),
		Uptime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_uptime_seconds",
				Help:        "Uptime: Amount of seconds the FST has been up",
				ConstLabels: labels,
			},
			[]string{"node"},
		),
		Start: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "versions_start_seconds",
				Help:        "Start: Time when EOS was started.",
				ConstLabels: labels,
			},
			[]string{"mgm_version", "node", "geotag", "eos_v_fst", "xrd_v_fst", "kernel_v"},
		),
	}
}

func (o *VSCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		//	o.EOSmgm,
		//	o.Hostport,
		//	o.Geotag,
		o.Vsize,
		o.Rss,
		o.Threads,
		o.Versions,
		//	o.EOSfst,
		//	o.Xrootdfst,
		//	o.KernelV,
		o.Start,
		o.Uptime,
	}
}

func (o *VSCollector) collectVSDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListVS(context.Background())
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		// Versions

		versions, err := strconv.ParseFloat("1", 64)
		if err == nil {
			o.Versions.WithLabelValues(m.EOSmgm, m.Hostname, m.Port, m.Geotag, m.EOSfst, m.Xrootdfst, m.KernelV).Set(versions)
		}

		// Uptime

		uptime, err := strconv.ParseFloat(m.Uptime, 64)
		if err == nil {
			o.Uptime.WithLabelValues(m.Hostname).Set(uptime*3600*24)
		}
	}

	return nil

} // collectVSDF()

// Describe sends the descriptors of each VSCollector related metrics we have defined
func (o *VSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *VSCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectVSDF(); err != nil {
		log.Println("failed collecting space metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
