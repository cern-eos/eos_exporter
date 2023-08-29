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
	CfgStatus             *prometheus.GaugeVec
	Nofs                  *prometheus.GaugeVec
	HeartBeatDelta        *prometheus.GaugeVec
	SumStatStatfsFree     *prometheus.GaugeVec
	SumStatStatfsUsed     *prometheus.GaugeVec
	SumStatStatfsTotal    *prometheus.GaugeVec
	SumStatStatFilesFree  *prometheus.GaugeVec
	SumStatStatFilesUsed  *prometheus.GaugeVec
	SumStatStatFilesTotal *prometheus.GaugeVec
	SumStatRopen          *prometheus.GaugeVec
	SumStatWopen          *prometheus.GaugeVec
	CfgStatSysThreads     *prometheus.GaugeVec
	CfgStatSysVsize       *prometheus.GaugeVec
	CfgStatSysRss         *prometheus.GaugeVec
	CfgStatSysSockets     *prometheus.GaugeVec
	SumStatNetInratemib   *prometheus.GaugeVec
	SumStatNetOutratemib  *prometheus.GaugeVec
	Info                  *prometheus.GaugeVec
}

/*
sample line
=========
type=nodesview
hostport=st-home-84364169.cern.ch:1150
status=online
cfg.status=on
cfg.txgw=off
heartbeatdelta=2
nofs=1
avg.stat.disk.load=0.00
sig.stat.disk.load=0.00
sum.stat.disk.readratemb=0
sum.stat.disk.writeratemb=0
cfg.stat.net.ethratemib=1192
cfg.stat.net.inratemib=3.94087
cfg.stat.net.outratemib=0.0853667
sum.stat.ropen=0 sum.stat.wopen=0
sum.stat.statfs.freebytes=799474651136
sum.stat.statfs.usedbytes=298795008
sum.stat.statfs.capacity=799773446144
sum.stat.usedfiles=4531 sum.stat.statfs.ffree=195338475
sum.stat.statfs.fused=14101
sum.stat.statfs.files=195352576
sum.stat.balancer.running=0
stat.gw.queued=
cfg.stat.sys.vsize=1289302016
cfg.stat.sys.rss=58286080
cfg.stat.sys.threads=123
cfg.stat.sys.sockets=57
cfg.stat.sys.eos.version=4.8.91-1
cfg.stat.sys.xrootd.version=v4.12.8
cfg.stat.sys.kernel=3.10.0-1160.66.1.el7.x86_64
cfg.stat.sys.eos.start=Wed%20Sep%2028%2020:04:54%202022
cfg.stat.sys.uptime=%2017:15:28%20up%20124%20days,%2010:47,%20%200%20users,%20%20load%20average:%2025.36,%2024.83,%2024.21
sum.stat.disk.iops?configstatus@rw=990
sum.stat.disk.bw?configstatus@rw=387
cfg.stat.geotag=0513::R::0050::CB11
cfg.gw.rate=120
cfg.gw.ntx=10
*/

// NewNodeCollector creates an cluster of the NodeCollector
func NewNodeCollector(cluster string) *NodeCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	return &NodeCollector{
		Status: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_status",
				Help:        "Node status: 1: online, 0: offline",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		CfgStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_cfgstatus",
				Help:        "Node config status: 1: on, 0: off",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		HeartBeatDelta: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_heartbeatdelta_seconds",
				Help:        "Node heart beat delta",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
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
		CfgStatSysVsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_vsize",
				Help:        "Node virtual memory size",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		CfgStatSysRss: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_rss",
				Help:        "Node resident memory set size",
				ConstLabels: labels,
			},
			[]string{"node", "port"},
		),
		CfgStatSysSockets: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_sockets",
				Help:        "Node Number of sockets",
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
		Info: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "node_info",
				Help:        "Node metadata",
				ConstLabels: labels,
			},
			[]string{"node", "port", "eos_version", "xrootd_version", "kernel", "geotag"},
		),
	}
}

func (o *NodeCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Status,
		o.CfgStatus,
		o.Nofs,
		o.HeartBeatDelta,
		o.SumStatStatfsFree,
		o.SumStatStatfsUsed,
		o.SumStatStatfsTotal,
		o.SumStatStatFilesFree,
		o.SumStatStatFilesUsed,
		o.SumStatStatFilesTotal,
		o.SumStatRopen,
		o.SumStatWopen,
		o.CfgStatSysThreads,
		o.CfgStatSysVsize,
		o.CfgStatSysRss,
		o.CfgStatSysSockets,
		o.SumStatNetInratemib,
		o.SumStatNetOutratemib,
		o.Info,
	}
}

func (o *NodeCollector) collectNodeDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
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

		// Status: 1: online, 0: offline

		var status int

		switch stat := m.Status; stat {
		case "online":
			status = 1
		case "offline":
			status = 0
		}

		o.Status.Reset()
		o.Status.WithLabelValues(m.Host, m.Port).Set(float64(status))

		// Config status: 1: on, 0: off

		var cfg_status int

		switch stat := m.CfgStatus; stat {
		case "on":
			cfg_status = 1
		case "off":
			cfg_status = 0
		}

		o.CfgStatus.Reset()
		o.CfgStatus.WithLabelValues(m.Host, m.Port).Set(float64(cfg_status))

		heartbeatdelta, err := strconv.ParseFloat(m.HeartBeatDelta, 64)
		if err == nil {
			o.HeartBeatDelta.Reset()
			o.HeartBeatDelta.WithLabelValues(m.Host, m.Port).Set(heartbeatdelta)
		}

		nofs, err := strconv.ParseFloat(m.Nofs, 64)
		if err == nil {
			o.Nofs.Reset()
			o.Nofs.WithLabelValues(m.Host, m.Port).Set(nofs)
		}

		fbytes, err := strconv.ParseFloat(m.SumStatStatfsFree, 64)
		if err == nil {
			o.SumStatStatfsFree.Reset()
			o.SumStatStatfsFree.WithLabelValues(m.Host, m.Port).Set(fbytes)
		}

		ubytes, err := strconv.ParseFloat(m.SumStatStatfsUsed, 64)
		if err == nil {
			o.SumStatStatfsUsed.Reset()
			o.SumStatStatfsUsed.WithLabelValues(m.Host, m.Port).Set(ubytes)
		}

		tbytes, err := strconv.ParseFloat(m.SumStatStatfsTotal, 64)
		if err == nil {
			o.SumStatStatfsTotal.Reset()
			o.SumStatStatfsTotal.WithLabelValues(m.Host, m.Port).Set(tbytes)
		}

		ffiles, err := strconv.ParseFloat(m.SumStatStatFilesFree, 64)
		if err == nil {
			o.SumStatStatFilesFree.Reset()
			o.SumStatStatFilesFree.WithLabelValues(m.Host, m.Port).Set(ffiles)
		}

		ufiles, err := strconv.ParseFloat(m.SumStatStatFilesUsed, 64)
		if err == nil {
			o.SumStatStatFilesUsed.Reset()
			o.SumStatStatFilesUsed.WithLabelValues(m.Host, m.Port).Set(ufiles)
		}

		tfiles, err := strconv.ParseFloat(m.SumStatStatFilesTotal, 64)
		if err == nil {
			o.SumStatStatFilesTotal.Reset()
			o.SumStatStatFilesTotal.WithLabelValues(m.Host, m.Port).Set(tfiles)
		}

		ropen, err := strconv.ParseFloat(m.SumStatRopen, 64)
		if err == nil {
			o.SumStatRopen.Reset()
			o.SumStatRopen.WithLabelValues(m.Host, m.Port).Set(ropen)
		}

		wopen, err := strconv.ParseFloat(m.SumStatWopen, 64)
		if err == nil {
			o.SumStatWopen.Reset()
			o.SumStatWopen.WithLabelValues(m.Host, m.Port).Set(wopen)
		}

		netin, err := strconv.ParseFloat(m.SumStatNetInratemib, 64)
		if err == nil {
			o.SumStatNetInratemib.Reset()
			o.SumStatNetInratemib.WithLabelValues(m.Host, m.Port).Set(netin)
		}

		netout, err := strconv.ParseFloat(m.SumStatNetOutratemib, 64)
		if err == nil {
			o.SumStatNetOutratemib.Reset()
			o.SumStatNetOutratemib.WithLabelValues(m.Host, m.Port).Set(netout)
		}

		threads, err := strconv.ParseFloat(m.CfgStatSysThreads, 64)
		if err == nil {
			o.CfgStatSysThreads.Reset()
			o.CfgStatSysThreads.WithLabelValues(m.Host, m.Port).Set(threads)
		}

		vsize, err := strconv.ParseFloat(m.CfgStatSysVsize, 64)
		if err == nil {
			o.CfgStatSysVsize.Reset()
			o.CfgStatSysVsize.WithLabelValues(m.Host, m.Port).Set(vsize)
		}

		rss, err := strconv.ParseFloat(m.CfgStatSysRss, 64)
		if err == nil {
			o.CfgStatSysRss.Reset()
			o.CfgStatSysRss.WithLabelValues(m.Host, m.Port).Set(rss)
		}

		sockets, err := strconv.ParseFloat(m.CfgStatSysSockets, 64)
		if err == nil {
			o.CfgStatSysSockets.Reset()
			o.CfgStatSysSockets.WithLabelValues(m.Host, m.Port).Set(sockets)
		}

		// We send just a dummy 1 as value for the eos_node_info metric, and metadata on labels
		o.Info.WithLabelValues(m.Host, m.Port, m.EOSVersion, m.XRootDVersion, m.Kernel, m.Geotag).Set(1)
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
