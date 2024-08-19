package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/cern-eos/eos_exporter/eosclient"
)

/*
type=spaceview
name=default
cfg.groupsize=11
cfg.groupmod=50
nofs=337
avg.stat.disk.load=0.27
sig.stat.disk.load=0.38
sum.stat.disk.readratemb=4481
sum.stat.disk.writeratemb=39
sum.stat.net.ethratemib=13708
sum.stat.net.inratemib=32
sum.stat.net.outratemib=32
sum.stat.ropen=72
sum.stat.wopen=77
sum.stat.statfs.usedbytes=1442925699514368
sum.stat.statfs.freebytes=1630645688213504
sum.stat.statfs.freebytes?configstatus@rw=1624647716261888
sum.stat.statfs.capacity=3073571387727872
sum.stat.usedfiles=386144110
sum.stat.statfs.ffiles=0
sum.stat.statfs.files=150111401344
sched.capacity=1617927719616512
sum.stat.statfs.capacity?configstatus@rw=3060852351262720
sum.<n>?configstatus@rw=336
cfg.quota=on
cfg.nominalsize=???
cfg.balancer=on
cfg.balancer.threshold=20
sum.stat.balancer.running=19
sum.stat.disk.iops?configstatus@rw=22960
sum.stat.disk.bw?configstatus@rw=63069
*/

type SpaceCollector struct {
	*CollectorOpts
	CfgGroupSize                         *prometheus.GaugeVec
	CfgGroupMod                          *prometheus.GaugeVec
	Nofs                                 *prometheus.GaugeVec
	AvgStatDiskLoad                      *prometheus.GaugeVec
	SigStatDiskLoad                      *prometheus.GaugeVec
	SumStatDiskReadratemb                *prometheus.GaugeVec
	SumStatDiskWriteratemb               *prometheus.GaugeVec
	SumStatNetEthratemib                 *prometheus.GaugeVec
	SumStatNetInratemib                  *prometheus.GaugeVec
	SumStatNetOutratemib                 *prometheus.GaugeVec
	SumStatRopen                         *prometheus.GaugeVec
	SumStatWopen                         *prometheus.GaugeVec
	SumStatStatfsUsedbytes               *prometheus.GaugeVec
	SumStatStatfsFreebytes               *prometheus.GaugeVec
	SumStatStatfsCapacity                *prometheus.GaugeVec
	SumStatUsedfiles                     *prometheus.GaugeVec
	SumStatStatfsFfiles                  *prometheus.GaugeVec
	SumStatStatfsFiles                   *prometheus.GaugeVec
	SumStatStatfsCapacityConfigstatusRw  *prometheus.GaugeVec
	SumNofsConfigstatusRw                *prometheus.GaugeVec
	CfgQuota                             *prometheus.GaugeVec
	CfgNominalsize                       *prometheus.GaugeVec
	CfgBalancer                          *prometheus.GaugeVec
	CfgBalancerThreshold                 *prometheus.GaugeVec
	SumStatBalancerRunning               *prometheus.GaugeVec
	SumStatDrainerRunning                *prometheus.GaugeVec
	SumStatDiskIopsConfigstatusRw        *prometheus.GaugeVec
	SumStatDiskBwConfigstatusRw          *prometheus.GaugeVec
	SumStatStatfsFreebytesConfigstatusRw *prometheus.GaugeVec
}

// NewSpaceCollector creates an cluster of the SpaceCollector
func NewSpaceCollector(opts *CollectorOpts) *SpaceCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &SpaceCollector{
		CollectorOpts: opts,
		CfgGroupSize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_groupsize",
				Help:        "Space Group Size",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgGroupMod: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_groupmod",
				Help:        "Space Group Mod",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		Nofs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_nofs",
				Help:        "Space Number of filesystems",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		AvgStatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_load_avg",
				Help:        "Space Avg disk load",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SigStatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_load_sig",
				Help:        "Space Sig disk load",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskReadratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_readratemb",
				Help:        "Space Disk Read Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskWriteratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_writeratemb",
				Help:        "Space Sum Disk Write Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetEthratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_net_ethratemib",
				Help:        "Space Net Eth Rate in MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetInratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_net_inratemib",
				Help:        "Space Net In Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetOutratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_net_outratemib",
				Help:        "Space Net Out Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatRopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_ropen",
				Help:        "Space Open reads",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatWopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_disk_wopen",
				Help:        "Space Open writes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsUsedbytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_usedbytes",
				Help:        "Space StatFs Used Bytes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFreebytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_freebytes",
				Help:        "Space StatFs Free Bytes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsCapacity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_sizebytes",
				Help:        "Space StatFs Size",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatUsedfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_usedfiles",
				Help:        "Space Used Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_freefiles",
				Help:        "Space Free Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_files",
				Help:        "Space Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsCapacityConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_statfs_sizebytes_configrw",
				Help:        "Space StatFs Capacity ConfigStatus RW",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumNofsConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_nofs_configrw",
				Help:        "Space Number of filesystems in FS with configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgQuota: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_quota",
				Help:        "Space Quota Status: 0=off, 1=on",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgNominalsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_nominalsize",
				Help:        "Space Nominal Size: 0=not defined",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgBalancer: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_balancer_status",
				Help:        "Space Group Balancer Status: 0=off, 1=on",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgBalancerThreshold: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_balancer_threshold",
				Help:        "Space Group Balancer Threshold",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatBalancerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_balancer_running",
				Help:        "Space Stat Balancer Running",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDrainerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_drainer_running",
				Help:        "Space Stat Drainer Running",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskIopsConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_disk_iops_configrw",
				Help:        "Space Stat Disk IOPS configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskBwConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_disk_bw_configrw",
				Help:        "Space Stat Disk Bandwidth configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFreebytesConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_statfs_freebytes_configrw",
				Help:        "Space free bytes counting on filesystem in rw mode",
				ConstLabels: labels,
			},
			[]string{"space"},
		),

		// SumStatStatfsFreebytesConfigstatusRw
	}
}

func (o *SpaceCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CfgGroupSize,
		o.CfgGroupMod,
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
		o.SumStatStatfsFfiles, // seems that this metric is broken in EOS
		o.SumStatStatfsFiles,
		o.SumStatStatfsCapacityConfigstatusRw,
		o.SumNofsConfigstatusRw,
		o.CfgQuota,
		o.CfgNominalsize,
		o.CfgBalancer,
		o.CfgBalancerThreshold,
		o.SumStatBalancerRunning,
		o.SumStatDrainerRunning,
		o.SumStatDiskIopsConfigstatusRw,
		o.SumStatDiskBwConfigstatusRw,
		o.SumStatStatfsFreebytesConfigstatusRw,
	}
}

func (o *SpaceCollector) collectSpaceDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListSpace(context.Background(), "root")
	if err != nil {
		return err
	}

	// reset gauges (to drop metrics of deleted spaces)

	o.CfgGroupSize.Reset()
	o.CfgGroupMod.Reset()
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
	o.SumStatStatfsFfiles.Reset()
	o.SumStatStatfsFiles.Reset()
	o.SumStatStatfsCapacityConfigstatusRw.Reset()
	o.SumNofsConfigstatusRw.Reset()
	o.CfgQuota.Reset()
	o.CfgNominalsize.Reset()
	o.CfgBalancer.Reset()
	o.CfgBalancerThreshold.Reset()
	o.SumStatBalancerRunning.Reset()
	o.SumStatDrainerRunning.Reset()
	o.SumStatDiskIopsConfigstatusRw.Reset()
	o.SumStatDiskBwConfigstatusRw.Reset()
	o.SumStatStatfsFreebytesConfigstatusRw.Reset()

	for _, m := range mds {

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

		files, err := strconv.ParseFloat(m.SumStatStatfsFiles, 64)
		if err == nil {
			o.SumStatStatfsFiles.WithLabelValues(m.Name).Set(files)
		}

		caprw, err := strconv.ParseFloat(m.SumStatStatfsCapacityConfigstatusRw, 64)
		if err == nil {
			o.SumStatStatfsCapacityConfigstatusRw.WithLabelValues(m.Name).Set(caprw)
		}

		nofsrw, err := strconv.ParseFloat(m.SumNofsConfigstatusRw, 64)
		if err == nil {
			o.SumNofsConfigstatusRw.WithLabelValues(m.Name).Set(nofsrw)
		}

		balr, err := strconv.ParseFloat(m.SumStatBalancerRunning, 64)
		if err == nil {
			o.SumStatBalancerRunning.WithLabelValues(m.Name).Set(balr)
		}

		drainr, err := strconv.ParseFloat(m.SumStatDrainerRunning, 64)
		if err == nil {
			o.SumStatDrainerRunning.WithLabelValues(m.Name).Set(drainr)
		}

		iopsrw, err := strconv.ParseFloat(m.SumStatDiskIopsConfigstatusRw, 64)
		if err == nil {
			o.SumStatDiskIopsConfigstatusRw.WithLabelValues(m.Name).Set(iopsrw)
		}

		bwrw, err := strconv.ParseFloat(m.SumStatDiskBwConfigstatusRw, 64)
		if err == nil {
			o.SumStatDiskBwConfigstatusRw.WithLabelValues(m.Name).Set(bwrw)
		}

		// Balancer Status

		balancer_status := 0
		switch stat := m.CfgBalancer; stat {
		case "on":
			balancer_status = 1
		default:
			balancer_status = 0
		}

		o.CfgBalancer.WithLabelValues(m.Name).Set(float64(balancer_status))

		balt, err := strconv.ParseFloat(m.CfgBalancerThreshold, 64)
		if err == nil {
			o.CfgBalancerThreshold.WithLabelValues(m.Name).Set(balt)
		}

		gsize, err := strconv.ParseFloat(m.CfgGroupSize, 64)
		if err == nil {
			o.CfgGroupSize.WithLabelValues(m.Name).Set(gsize)
		}

		gmod, err := strconv.ParseFloat(m.CfgGroupMod, 64)
		if err == nil {
			o.CfgGroupMod.WithLabelValues(m.Name).Set(gmod)
		}

		// Quota Status

		quota_status := 0
		switch stat := m.CfgQuota; stat {
		case "on":
			quota_status = 1
		default:
			quota_status = 0
		}

		o.CfgQuota.WithLabelValues(m.Name).Set(float64(quota_status))

		// convert nominal size 0 if not defined.
		if m.CfgNominalsize != "???" {
			nomsize, err := strconv.ParseFloat(m.CfgNominalsize, 64)
			if err == nil {
				o.CfgNominalsize.WithLabelValues(m.Name).Set(nomsize)
			}
		} else {
			o.CfgNominalsize.WithLabelValues(m.Name).Set(0)
		}

		fbytesRW, err := strconv.ParseFloat(m.SumStatStatfsFreebytesConfigstatusRw, 64)
		if err == nil {
			o.SumStatStatfsFreebytesConfigstatusRw.WithLabelValues(m.Name).Set(fbytesRW)
		}

	}

	return nil

} // collectSpaceDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *SpaceCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *SpaceCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectSpaceDF(); err != nil {
		log.Println("failed collecting space metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
