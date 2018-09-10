package collector

import (
	"log"
	"context"
	"github.com/prometheus/client_golang/prometheus"
	"eos_exporter/eosclient"
	"strconv"
)

type SpaceCollector struct {

	CfgGroupSize 							*prometheus.GaugeVec
	CfgGroupMod			  					*prometheus.GaugeVec
	Nofs	  								*prometheus.GaugeVec
	AvgStatDiskLoad 	    				*prometheus.GaugeVec
	SigStatDiskLoad  						*prometheus.GaugeVec
	SumStatDiskReadratemb  					*prometheus.GaugeVec
	SumStatDiskWriteratemb 					*prometheus.GaugeVec
	SumStatNetEthratemib		  			*prometheus.GaugeVec
	SumStatNetInratemib		  				*prometheus.GaugeVec
	SumStatNetOutratemib	  				*prometheus.GaugeVec
	SumStatRopen	  						*prometheus.GaugeVec
	SumStatWopen  							*prometheus.GaugeVec
	SumStatStatfsUsedbytes 					*prometheus.GaugeVec
    SumStatStatfsFreebytes 					*prometheus.GaugeVec
	SumStatStatfsCapacity 					*prometheus.GaugeVec
	SumStatUsedfiles 						*prometheus.GaugeVec
	SumStatStatfsFfiles 					*prometheus.GaugeVec
	SumStatStatfsFiles 						*prometheus.GaugeVec
	SumStatStatfsCapacityConfigstatusRw 	*prometheus.GaugeVec
	SumNofsConfigstatusRw 					*prometheus.GaugeVec
	CfgQuota 								*prometheus.GaugeVec
	CfgNominalsize 							*prometheus.GaugeVec
	CfgBalancer 							*prometheus.GaugeVec
	CfgBalancerThreshold 					*prometheus.GaugeVec
	SumStatBalancerRunning 					*prometheus.GaugeVec
	SumStatDrainerRunning 					*prometheus.GaugeVec
	SumStatDiskIopsConfigstatusRw 			*prometheus.GaugeVec
	SumStatDiskBwConfigstatusRw 			*prometheus.GaugeVec
}

//NewSpaceCollector creates an instance of the SpaceCollector
func NewSpaceCollector(cluster string) *SpaceCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &SpaceCollector{

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
				Name:        "space_avg_stat_disk_load",
				Help:        "Space Avg Stat disk load",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SigStatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_sig_stat_disk_load",
				Help:        "Space Sig Stat disk load",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskReadratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_sum_stat_disk_readratemb",
				Help:        "Space Sum Stat Disk Read Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskWriteratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_sum_stat_disk_writeratemb",
				Help:        "Space Sum Stat Disk Write Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetEthratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_net_ethratemib",
				Help:        "Space Stat Net Eth Rate in MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetInratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_net_inratemib",
				Help:        "Space Stat Net In Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatNetOutratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_net_outratemib",
				Help:        "Space Stat Net Out Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatRopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_sum_stat_ropen",
				Help:        "Space Open reads",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatWopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_sum_stat_wopen",
				Help:        "Space Open writes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsUsedbytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_statfs_usedbytes",
				Help:        "Space StatFs Used Bytes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFreebytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_statfs_freebytes",
				Help:        "Space StatFs Free Bytes",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsCapacity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_statfs_capacity_bytes",
				Help:        "Space StatFs Capacity",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatUsedfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_used_files",
				Help:        "Space Used Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_stafs_ffiles",
				Help:        "Space F-Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_stafs_files",
				Help:        "Space Files",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatStatfsCapacityConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "space_stat_statfs_capacity_configstatus_rw",
				Help:        "Space StatFs Capacity ConfigStatus RW",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumNofsConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_nofs_configstatus_rw",
				Help:        "Space Number of filesystems in FS with configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgQuota: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_quota",
				Help:        "Space Quota",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgNominalsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_nominalsize",
				Help:        "Space Nominal Size",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		CfgBalancer: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_cfg_balancer",
				Help:        "Space Group Balancer",
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
				Name:        "space_sum_stat_balancer_running",
				Help:        "Space Stat Balancer Running",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDrainerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_sum_stat_drainer_running",
				Help:        "Space Stat Drainer Running",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskIopsConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_sum_stat_disk_iops_configstatus_rw",
				Help:        "Space Stat Disk IOPS configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
		SumStatDiskBwConfigstatusRw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "space_sum_stat_disk_bandwidth_configstatus_rw",
				Help:        "Space Stat Disk Bandwidth configstatus=rw",
				ConstLabels: labels,
			},
			[]string{"space"},
		),
	}
}

func (o *SpaceCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CfgGroupSize, //unrelevant
		o.CfgGroupMod, //unrelevant
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
		o.SumStatStatfsFreebytes, // easy to calculate
		o.SumStatStatfsCapacity,
		o.SumStatUsedfiles,
		o.SumStatStatfsFfiles, //not sure what is this
		o.SumStatStatfsFiles,
		o.SumStatStatfsCapacityConfigstatusRw,
		o.SumNofsConfigstatusRw,
		o.CfgQuota, //unrelevant
		o.CfgNominalsize, //unrelevant
		o.CfgBalancer, //unrelevant
		o.CfgBalancerThreshold, //unrelevant
		o.SumStatBalancerRunning,
		o.SumStatDrainerRunning,
		o.SumStatDiskIopsConfigstatusRw,
		o.SumStatDiskBwConfigstatusRw,
	}
}

func (o *SpaceCollector) collectSpaceDF() error {

	opt := &eosclient.Options{URL: "root://eospps.cern.ch"}
    client, err := eosclient.New(opt)
    if err != nil {
    	panic(err)
    }

    mds, err := client.ListSpace(context.Background(), "root")
    if err != nil {
    	panic(err)
    }

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
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
