package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type GroupCollector struct {
	*CollectorOpts
	Name                   *prometheus.GaugeVec
	CfgStatus              *prometheus.GaugeVec
	Nofs                   *prometheus.GaugeVec
	AvgStatDiskLoad        *prometheus.GaugeVec
	SigStatDiskLoad        *prometheus.GaugeVec
	SumStatDiskReadratemb  *prometheus.GaugeVec
	SumStatDiskWriteratemb *prometheus.GaugeVec
	SumStatNetEthratemib   *prometheus.GaugeVec
	SumStatNetInratemib    *prometheus.GaugeVec
	SumStatNetOutratemib   *prometheus.GaugeVec
	SumStatRopen           *prometheus.GaugeVec
	SumStatWopen           *prometheus.GaugeVec
	SumStatStatfsUsedbytes *prometheus.GaugeVec
	SumStatStatfsFreebytes *prometheus.GaugeVec
	SumStatStatfsCapacity  *prometheus.GaugeVec
	SumStatUsedfiles       *prometheus.GaugeVec
	SumStatStatfsFfree     *prometheus.GaugeVec
	SumStatStatfsFiles     *prometheus.GaugeVec
	DevStatStatfsFilled    *prometheus.GaugeVec
	AvgStatStatfsFilled    *prometheus.GaugeVec
	SigStatStatfsFilled    *prometheus.GaugeVec
	CfgStatBalancing       *prometheus.GaugeVec
	SumStatBalancerRunning *prometheus.GaugeVec
	SumStatDrainerRunning  *prometheus.GaugeVec
}

// NewGroupCollector creates an cluster of the GroupCollector and instantiates
// the individual metrics that show information about the Group.
func NewGroupCollector(opts *CollectorOpts) *GroupCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &GroupCollector{
		CollectorOpts: opts,
		CfgStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_cfg_status",
				Help:        "Group Status 0=off, 1=on",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		Nofs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_nofs",
				Help:        "Number of filesystems in the group",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		AvgStatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_load_avg",
				Help:        "Group Avg Stat disk load",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SigStatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_load_sig",
				Help:        "Group Sig Stat disk load",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatDiskReadratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_readratemb",
				Help:        "Group Sum Stat Disk Read Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatDiskWriteratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_writeratemb",
				Help:        "Group Sum Stat Disk Write Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatNetEthratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_net_ethratemib",
				Help:        "Group Stat Net Eth Rate in MiB/s",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatNetInratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_net_inratemib",
				Help:        "Group Stat Net In Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatNetOutratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_net_outratemib",
				Help:        "Group Stat Net Out Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatRopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_ropen",
				Help:        "Group Open reads",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatWopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_disk_wopen",
				Help:        "Group Open writes",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatStatfsUsedbytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_usedbytes",
				Help:        "Group StatFs Used Bytes",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatStatfsFreebytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_freebytes",
				Help:        "Group StatFs Free Bytes",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatStatfsCapacity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_sizebytes",
				Help:        "Group StatFs Capacity",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatUsedfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_usedfiles",
				Help:        "Group Used Files",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatStatfsFfree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_freefiles",
				Help:        "Group Free-Files",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatStatfsFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_totalfiles",
				Help:        "Group Files",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		DevStatStatfsFilled: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_filled_dev",
				Help:        "Group Dev Filled",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		AvgStatStatfsFilled: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_filled_avg",
				Help:        "Group Avg Filled",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SigStatStatfsFilled: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "group_statfs_filled_sig",
				Help:        "Group Sig Filled",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		CfgStatBalancing: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "group_balancer_status",
				Help:        "Status of group balancing 0=idle, 1=balancing, 2=drainwait",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatBalancerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "group_balancer_running",
				Help:        "Group Stat Balancer Running",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		SumStatDrainerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "group_drainer_running",
				Help:        "Group Stat Drainer Running",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
	}
}

func (o *GroupCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CfgStatus,
		o.Nofs,
		o.AvgStatDiskLoad,
		o.SigStatDiskLoad,
		o.SumStatDiskReadratemb,
		o.SumStatDiskWriteratemb,
		o.SumStatNetEthratemib,
		o.SumStatNetInratemib,
		o.SumStatNetOutratemib,
		o.SumStatRopen,
		o.SumStatWopen,
		o.SumStatStatfsUsedbytes,
		o.SumStatStatfsFreebytes,
		o.SumStatStatfsCapacity,
		o.SumStatUsedfiles,
		o.SumStatStatfsFfree,
		o.SumStatStatfsFiles,
		o.DevStatStatfsFilled,
		o.AvgStatStatfsFilled,
		o.SigStatStatfsFilled,
		o.CfgStatBalancing,
		o.SumStatBalancerRunning,
		o.SumStatDrainerRunning,
	}
}

func (o *GroupCollector) collectGroupDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListGroup(context.Background(), "root")
	if err != nil {
		log.Println(err)
		return err
	}

	// Reset gauge metrics to remove metrics of deleted groups

	o.CfgStatus.Reset()
	o.Nofs.Reset()
	o.AvgStatDiskLoad.Reset()
	o.SigStatDiskLoad.Reset()
	o.SumStatDiskReadratemb.Reset()
	o.SumStatDiskWriteratemb.Reset()
	o.SumStatNetEthratemib.Reset()
	o.SumStatNetInratemib.Reset()
	o.SumStatNetOutratemib.Reset()
	o.SumStatRopen.Reset()
	o.SumStatWopen.Reset()
	o.SumStatStatfsUsedbytes.Reset()
	o.SumStatStatfsFreebytes.Reset()
	o.SumStatStatfsCapacity.Reset()
	o.SumStatUsedfiles.Reset()
	o.SumStatStatfsFfree.Reset()
	o.SumStatStatfsFiles.Reset()
	o.DevStatStatfsFilled.Reset()
	o.AvgStatStatfsFilled.Reset()
	o.SigStatStatfsFilled.Reset()
	o.CfgStatBalancing.Reset()
	o.SumStatBalancerRunning.Reset()
	o.SumStatDrainerRunning.Reset()

	for _, m := range mds {

		cfgstatus := 0
		if m.CfgStatus == "on" {
			cfgstatus = 1
		}

		status := float64(cfgstatus)
		o.CfgStatus.WithLabelValues(m.Name).Set(status)

		nofs, err := strconv.ParseFloat(m.Nofs, 64)
		if err == nil {
			o.Nofs.WithLabelValues(m.Name).Set(nofs)
		}

		avgdl, err := strconv.ParseFloat(m.AvgStatDiskLoad, 64)
		if err == nil {
			o.AvgStatDiskLoad.WithLabelValues(m.Name).Set(avgdl)
		}

		sigdl, err := strconv.ParseFloat(m.SigStatDiskLoad, 64)
		if err == nil {
			o.SigStatDiskLoad.WithLabelValues(m.Name).Set(sigdl)
		}

		sumdiskr, err := strconv.ParseFloat(m.SumStatDiskReadratemb, 64)
		if err == nil {
			o.SumStatDiskReadratemb.WithLabelValues(m.Name).Set(sumdiskr)
		}

		sumdiskw, err := strconv.ParseFloat(m.SumStatDiskWriteratemb, 64)
		if err == nil {
			o.SumStatDiskWriteratemb.WithLabelValues(m.Name).Set(sumdiskw)
		}

		sumethrate, err := strconv.ParseFloat(m.SumStatNetEthratemib, 64)
		if err == nil {
			o.SumStatNetEthratemib.WithLabelValues(m.Name).Set(sumethrate)
		}

		suminrate, err := strconv.ParseFloat(m.SumStatNetInratemib, 64)
		if err == nil {
			o.SumStatNetInratemib.WithLabelValues(m.Name).Set(suminrate)
		}

		sumoutrate, err := strconv.ParseFloat(m.SumStatNetOutratemib, 64)
		if err == nil {
			o.SumStatNetOutratemib.WithLabelValues(m.Name).Set(sumoutrate)
		}

		ropen, err := strconv.ParseFloat(m.SumStatRopen, 64)
		if err == nil {
			o.SumStatRopen.WithLabelValues(m.Name).Set(ropen)
		}

		wopen, err := strconv.ParseFloat(m.SumStatWopen, 64)
		if err == nil {
			o.SumStatWopen.WithLabelValues(m.Name).Set(wopen)
		}

		usedb, err := strconv.ParseFloat(m.SumStatStatfsUsedbytes, 64)
		if err == nil {
			o.SumStatStatfsUsedbytes.WithLabelValues(m.Name).Set(usedb)
		}

		fbytes, err := strconv.ParseFloat(m.SumStatStatfsFreebytes, 64)
		if err == nil {
			o.SumStatStatfsFreebytes.WithLabelValues(m.Name).Set(fbytes)
		}

		fscap, err := strconv.ParseFloat(m.SumStatStatfsCapacity, 64)
		if err == nil {
			o.SumStatStatfsCapacity.WithLabelValues(m.Name).Set(fscap)
		}

		ufiles, err := strconv.ParseFloat(m.SumStatUsedfiles, 64)
		if err == nil {
			o.SumStatUsedfiles.WithLabelValues(m.Name).Set(ufiles)
		}

		ffree, err := strconv.ParseFloat(m.SumStatStatfsFfree, 64)
		if err == nil {
			o.SumStatStatfsFfree.WithLabelValues(m.Name).Set(ffree)
		}

		files, err := strconv.ParseFloat(m.SumStatStatfsFiles, 64)
		if err == nil {
			o.SumStatStatfsFiles.WithLabelValues(m.Name).Set(files)
		}

		devfilled, err := strconv.ParseFloat(m.DevStatStatfsFilled, 64)
		if err == nil {
			o.DevStatStatfsFilled.WithLabelValues(m.Name).Set(devfilled)
		}

		avgfilled, err := strconv.ParseFloat(m.AvgStatStatfsFilled, 64)
		if err == nil {
			o.AvgStatStatfsFilled.WithLabelValues(m.Name).Set(avgfilled)
		}

		sigfilled, err := strconv.ParseFloat(m.SigStatStatfsFilled, 64)
		if err == nil {
			o.SigStatStatfsFilled.WithLabelValues(m.Name).Set(sigfilled)
		}

		// Balancer Status.

		balancer_status := 0
		switch stat := m.CfgStatBalancing; stat {
		case "idle":
			balancer_status = 0
		case "balancing":
			balancer_status = 1
		case "drainwait":
			balancer_status = 2
		default:
			balancer_status = 0
		}

		o.CfgStatBalancing.WithLabelValues(m.Name).Set(float64(balancer_status))

		balr, err := strconv.ParseFloat(m.SumStatBalancerRunning, 64)
		if err == nil {
			o.SumStatBalancerRunning.WithLabelValues(m.Name).Set(balr)
		}

		drainr, err := strconv.ParseFloat(m.SumStatDrainerRunning, 64)
		if err == nil {
			o.SumStatDrainerRunning.WithLabelValues(m.Name).Set(drainr)
		}
	}

	return nil

} // collectGroupDF()

// Describe sends the descriptors of each GroupCollector related metrics we have defined
func (o *GroupCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *GroupCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectGroupDF(); err != nil {
		log.Println("failed collecting group metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
