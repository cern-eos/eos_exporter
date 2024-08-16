package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type QuotaCollector struct {
	*CollectorOpts
	Space               *prometheus.GaugeVec
	Uid                 *prometheus.GaugeVec
	Gid                 *prometheus.GaugeVec
	UsedBytes           *prometheus.GaugeVec
	UsedLogicalBytes    *prometheus.GaugeVec
	UsedFiles           *prometheus.GaugeVec
	MaxBytes            *prometheus.GaugeVec
	MaxLogicalBytes     *prometheus.GaugeVec
	MaxFiles            *prometheus.GaugeVec
	PercentageUsedBytes *prometheus.GaugeVec
	StatusBytes         *prometheus.GaugeVec
	StatusFiles         *prometheus.GaugeVec
}

// NewGroupCollector creates an cluster of the GroupCollector and instantiates
// the individual metrics that show information about the Group.
func NewQuotaCollector(opts *CollectorOpts) *QuotaCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	return &QuotaCollector{
		CollectorOpts: opts,
		Uid: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_uid",
				Help:        "Uid of the quota node",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		Gid: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_gid",
				Help:        "Gid of the quota node",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		UsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_used_bytes",
				Help:        "Quota Used Bytes",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		UsedLogicalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_used_bytes",
				Help:        "Quota Used Bytes",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		UsedFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_used_files",
				Help:        "Quota Number of Used Files",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		MaxBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_max_bytes",
				Help:        "Quota Maximum Number of Bytes",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		MaxLogicalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_max_logical_bytes",
				Help:        "Quota maximum Logical Bytes",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		MaxFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_max_files",
				Help:        "Quota Maximum Number of Files",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		PercentageUsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_percentage_used_bytes",
				Help:        "Quota Percentage Used Bytes",
				ConstLabels: labels,
			},
			[]string{"quota"},
		),
		StatusBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_status_bytes",
				Help:        "Quota Status Bytes (ok or exceeded)",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
		StatusFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "quota_status_files",
				Help:        "Quota Status Files (ok or exceeded)",
				ConstLabels: labels,
			},
			[]string{"group"},
		),
	}
}

func (o *QuotaCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Uid,
		o.Gid,
		o.UsedBytes,
		o.UsedLogicalBytes,
		o.UsedFiles,
		o.MaxBytes,
		o.MaxFiles,
		o.PercentageUsedBytes,
		o.StatusBytes,
		o.StatusFiles,
	}
}

func (o *QuotaCollector) collectQuotaDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListQuota(context.Background(), "root")
	if err != nil {
		return err
	}

	// Reset gauge metrics to remove metrics of deleted groups

	o.Uid.Reset()
	o.Gid.Reset()
	o.UsedBytes.Reset()
	o.UsedLogicalBytes.Reset()
	o.UsedFiles.Reset()
	o.MaxBytes.Reset()
	o.MaxFiles.Reset()
	o.PercentageUsedBytes.Reset()
	o.StatusBytes.Reset()
	o.StatusFiles.Reset()

	for _, m := range mds {

		uid, err := strconv.ParseFloat(m.Uid, 64)
		if err == nil {
			o.Uid.WithLabelValues(m.QuotaNode).Set(uid)
		}

		gid, err := strconv.ParseFloat(m.Gid, 64)
		if err == nil {
			o.Gid.WithLabelValues(m.QuotaNode).Set(gid)
		}

		usedBytes, err := strconv.ParseFloat(m.UsedBytes, 64)
		if err == nil {
			o.UsedBytes.WithLabelValues(m.QuotaNode).Set(usedBytes)
		}

		usedLogicalBytes, err := strconv.ParseFloat(m.UsedLogicalBytes, 64)
		if err == nil {
			o.UsedLogicalBytes.WithLabelValues(m.QuotaNode).Set(usedLogicalBytes)
		}

		usedFiles, err := strconv.ParseFloat(m.UsedFiles, 64)
		if err == nil {
			o.UsedFiles.WithLabelValues(m.QuotaNode).Set(float64(usedFiles))
		}

		maxbytes, err := strconv.ParseFloat(m.MaxBytes, 64)
		if err == nil {
			o.MaxBytes.WithLabelValues(m.QuotaNode).Set(maxbytes)
		}

		maxfiles, err := strconv.ParseFloat(m.MaxFiles, 64)
		if err == nil {
			o.MaxFiles.WithLabelValues(m.QuotaNode).Set(maxfiles)
		}

		percentageusedbytes, err := strconv.ParseFloat(m.PercentageUsedBytes, 64)
		if err == nil {
			o.PercentageUsedBytes.WithLabelValues(m.QuotaNode).Set(percentageusedbytes)
		}

		statusBytes := 0
		if m.StatusBytes == "exceeded" {
			statusBytes = 1
		}
		statusB := float64(statusBytes)
		o.StatusBytes.WithLabelValues(m.QuotaNode).Set(statusB)

		statusFiles := 0
		if m.StatusFiles == "exceeded" {
			statusFiles = 1
		}
		statusF := float64(statusFiles)
		o.StatusFiles.WithLabelValues(m.QuotaNode).Set(statusF)

	}

	return nil

} // collectGroupDF()

// Describe sends the descriptors of each GroupCollector related metrics we have defined
func (o *QuotaCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *QuotaCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectQuotaDF(); err != nil {
		log.Println("failed collecting group metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
