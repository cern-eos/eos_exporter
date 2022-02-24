package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

const (
	nodeLabelFormat = "node.%v"
)

type NodeCollector struct {

	// UsedBytes displays the total used bytes in the Node
	Host                  *prometheus.GaugeVec
	Port                  *prometheus.GaugeVec
	Status                *prometheus.GaugeVec
	Nofs                  *prometheus.GaugeVec
	SumStatStatfsFree     *prometheus.GaugeVec
	SumStatStatfsUsed     *prometheus.GaugeVec
	SumStatStatfsTotal    *prometheus.GaugeVec
	SumStatStatFilesFree  *prometheus.GaugeVec
	SumStatStatFilesUsed  *prometheus.GaugeVec
	SumStatStatFilesTotal *prometheus.GaugeVec
	SumStatRopen          *prometheus.GaugeVec
	SumStatWopen          *prometheus.GaugeVec
	CfgStatSysThreads     *prometheus.GaugeVec
	SumStatNetInratemib   *prometheus.GaugeVec
	SumStatNetOutratemib  *prometheus.GaugeVec
}

//NewNodeCollector creates an cluster of the NodeCollector
func NewNodeCollector(cluster string) *NodeCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	return &NodeCollector{

		Nofs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_nofs",
				Help:        "Node Number of filesystems",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatfsFree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_freebytes",
				Help:        "Node Free Bytes",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatfsUsed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_usedbytes",
				Help:        "Node Used Bytes",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatfsTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_sizebytes",
				Help:        "Node Total Bytes",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatFilesFree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_freefiles",
				Help:        "Node Free Files",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatFilesUsed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_usedfiles",
				Help:        "Node Used Files",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatStatFilesTotal: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_statfs_totalfiles",
				Help:        "Node Total Files",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatRopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_disk_ropen",
				Help:        "Node Open reads",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatWopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_disk_wopen",
				Help:        "Node Open writes",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		CfgStatSysThreads: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_threads",
				Help:        "Node Number of threads",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatNetInratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_net_inratemib",
				Help:        "Node Net in Rate in Mib",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		SumStatNetOutratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_net_outratemib",
				Help:        "Node Net out Rate in Mib",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
	}
}

func (o *NodeCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Nofs,
		o.SumStatStatfsFree,
		o.SumStatStatfsUsed,
		o.SumStatStatfsTotal,
		o.SumStatStatFilesFree,
		o.SumStatStatFilesUsed,
		o.SumStatStatFilesTotal,
		o.SumStatRopen,
		o.SumStatWopen,
		o.CfgStatSysThreads,
		o.SumStatNetInratemib,
		o.SumStatNetOutratemib,
	}
}

func (o *NodeCollector) collectNodeDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListNode(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		nofs, err := strconv.ParseFloat(m.Nofs, 64)
		if err == nil {
			o.Nofs.WithLabelValues(m.Host, m.Port).Set(nofs)
		}

		fbytes, err := strconv.ParseFloat(m.SumStatStatfsFree, 64)
		if err == nil {
			o.SumStatStatfsFree.WithLabelValues(m.Host, m.Port).Set(fbytes)
		}

		ubytes, err := strconv.ParseFloat(m.SumStatStatfsUsed, 64)
		if err == nil {
			o.SumStatStatfsUsed.WithLabelValues(m.Host, m.Port).Set(ubytes)
		}

		tbytes, err := strconv.ParseFloat(m.SumStatStatfsTotal, 64)
		if err == nil {
			o.SumStatStatfsTotal.WithLabelValues(m.Host, m.Port).Set(tbytes)
		}

		ffiles, err := strconv.ParseFloat(m.SumStatStatFilesFree, 64)
		if err == nil {
			o.SumStatStatFilesFree.WithLabelValues(m.Host, m.Port).Set(ffiles)
		}

		ufiles, err := strconv.ParseFloat(m.SumStatStatFilesUsed, 64)
		if err == nil {
			o.SumStatStatFilesUsed.WithLabelValues(m.Host, m.Port).Set(ufiles)
		}

		tfiles, err := strconv.ParseFloat(m.SumStatStatFilesTotal, 64)
		if err == nil {
			o.SumStatStatFilesTotal.WithLabelValues(m.Host, m.Port).Set(tfiles)
		}

		ropen, err := strconv.ParseFloat(m.SumStatRopen, 64)
		if err == nil {
			o.SumStatRopen.WithLabelValues(m.Host, m.Port).Set(ropen)
		}

		wopen, err := strconv.ParseFloat(m.SumStatWopen, 64)
		if err == nil {
			o.SumStatWopen.WithLabelValues(m.Host, m.Port).Set(wopen)
		}

		netin, err := strconv.ParseFloat(m.SumStatNetInratemib, 64)
		if err == nil {
			o.SumStatNetInratemib.WithLabelValues(m.Host, m.Port).Set(netin)
		}

		netout, err := strconv.ParseFloat(m.SumStatNetOutratemib, 64)
		if err == nil {
			o.SumStatNetOutratemib.WithLabelValues(m.Host, m.Port).Set(netout)
		}

		threads, err := strconv.ParseFloat(m.CfgStatSysThreads, 64)
		if err == nil {
			o.CfgStatSysThreads.WithLabelValues(m.Host, m.Port).Set(threads)
		}
	}

	return nil

} // collectNodeDF()

// Describe sends the descriptors of each NodeCollector related metrics we have defined
func (o *NodeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *NodeCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectNodeDF(); err != nil {
		log.Println("failed collecting node metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
