package collector

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

type IOShapingCollector struct {
	*CollectorOpts

	RateBytes *prometheus.GaugeVec
	RateIops  *prometheus.GaugeVec

	FSRateBytes *prometheus.GaugeVec
	FSRateIops  *prometheus.GaugeVec

	AllRateBytes *prometheus.GaugeVec
	AllRateIops  *prometheus.GaugeVec

	// System metrics
	SystemLoopDurationUs   *prometheus.GaugeVec
	ReportsProcessedPerSec *prometheus.GaugeVec
}

func NewIOShapingCollector(opts *CollectorOpts) *IOShapingCollector {
	cluster := opts.Cluster
	labels := prometheus.Labels{"cluster": cluster}
	namespace := "eos"

	standardLabels := []string{"type", "id", "window_sec", "operation"}
	fsLabels := []string{"node_id", "fsid", "window_sec", "operation"}
	allLabels := []string{"node_id", "fsid", "app", "uid", "gid", "window_sec", "operation"}
	systemLabels := []string{"loop_name", "stat"}
	reportLabels := []string{"stat"}

	return &IOShapingCollector{
		CollectorOpts: opts,

		RateBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_rate_bytes",
			Help:        "IO shaping throughput in bytes per second",
			ConstLabels: labels,
		}, standardLabels),

		RateIops: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_rate_iops",
			Help:        "IO shaping operations per second",
			ConstLabels: labels,
		}, standardLabels),

		FSRateBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_fs_rate_bytes",
			Help:        "IO shaping filesystem throughput in bytes per second",
			ConstLabels: labels,
		}, fsLabels),

		FSRateIops: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_fs_rate_iops",
			Help:        "IO shaping filesystem operations per second",
			ConstLabels: labels,
		}, fsLabels),

		AllRateBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_all_rate_bytes",
			Help:        "IO shaping all-tags throughput in bytes per second",
			ConstLabels: labels,
		}, allLabels),

		AllRateIops: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_all_rate_iops",
			Help:        "IO shaping all-tags operations per second",
			ConstLabels: labels,
		}, allLabels),

		SystemLoopDurationUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_sys_loop_duration_microseconds",
			Help:        "System thread loop duration in microseconds",
			ConstLabels: labels,
		}, systemLabels),

		ReportsProcessedPerSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_reports_processed_per_sec",
			Help:        "FST IO reports processed per second",
			ConstLabels: labels,
		}, reportLabels),
	}
}

func (o *IOShapingCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.RateBytes, o.RateIops, o.FSRateBytes, o.FSRateIops, o.AllRateBytes, o.AllRateIops, o.SystemLoopDurationUs, o.ReportsProcessedPerSec,
	}
}

func (o *IOShapingCollector) collectIOShaping() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		return fmt.Errorf("failed to create eosclient: %w", err)
	}

	windows := []int{15, 300}

	for _, win := range windows {
		stats, err := client.ListIOShaping(context.Background(), win)
		if err != nil {
			log.Printf("failed to collect IO shaping for window %ds: %v", win, err)
			continue
		}

		for _, s := range stats {
			// Handle System Stats elegantly with labels
			if s.Type == "system" {
				setSysMetric := func(loopName, statName, valStr string) {
					if valStr == "" {
						return
					}
					if val, err := strconv.ParseFloat(valStr, 64); err == nil {
						o.SystemLoopDurationUs.WithLabelValues(loopName, statName).Set(val)
					}
				}

				setSysMetric("estimators", "median", s.EstimatorsLoopMedianUs)
				setSysMetric("estimators", "min", s.EstimatorsLoopMinUs)
				setSysMetric("estimators", "max", s.EstimatorsLoopMaxUs)

				setSysMetric("fst_limits", "median", s.FstLimitsLoopMedianUs)
				setSysMetric("fst_limits", "min", s.FstLimitsLoopMinUs)
				setSysMetric("fst_limits", "max", s.FstLimitsLoopMaxUs)

				// Restored Reports metric
				if s.ReportsProcessedPerSecMean != "" {
					if val, err := strconv.ParseFloat(s.ReportsProcessedPerSecMean, 64); err == nil {
						o.ReportsProcessedPerSec.WithLabelValues("mean").Set(val)
					}
				}

				continue // Skip the standard groupings for this system JSON object
			}

			// Handle Standard Groupings
			setMetric := func(vec *prometheus.GaugeVec, operation, valStr string) {
				if valStr == "" {
					return
				}
				if val, err := strconv.ParseFloat(valStr, 64); err == nil {
					vec.WithLabelValues(s.Type, s.ID, s.WindowSec, operation).Set(val)
				}
			}

			setMetric(o.RateBytes, "read", s.ReadRateBps)
			setMetric(o.RateBytes, "write", s.WriteRateBps)
			setMetric(o.RateIops, "read", s.ReadIops)
			setMetric(o.RateIops, "write", s.WriteIops)
		}
	}

	fsStats, err := client.ListIOShapingFS(context.Background())
	if err != nil {
		log.Printf("failed to collect IO shaping filesystem stats: %v", err)
		return nil
	}

	for _, s := range fsStats {
		setFSMetric := func(vec *prometheus.GaugeVec, operation, valStr string) {
			if valStr == "" {
				return
			}
			if val, err := strconv.ParseFloat(valStr, 64); err == nil {
				vec.WithLabelValues(s.NodeID, s.FSID, s.WindowSec, operation).Set(val)
			}
		}

		setFSMetric(o.FSRateBytes, "read", s.ReadRateBps)
		setFSMetric(o.FSRateBytes, "write", s.WriteRateBps)
		setFSMetric(o.FSRateIops, "read", s.ReadIops)
		setFSMetric(o.FSRateIops, "write", s.WriteIops)
	}

	allStats, err := client.ListIOShapingAll(context.Background())
	if err != nil {
		log.Printf("failed to collect IO shaping all-tags stats: %v", err)
		return nil
	}

	for _, s := range allStats {
		setAllMetric := func(vec *prometheus.GaugeVec, operation, valStr string) {
			if valStr == "" {
				return
			}
			if val, err := strconv.ParseFloat(valStr, 64); err == nil {
				vec.WithLabelValues(s.NodeID, s.FSID, s.App, s.UID, s.GID, s.WindowSec, operation).Set(val)
			}
		}

		setAllMetric(o.AllRateBytes, "read", s.ReadRateBps)
		setAllMetric(o.AllRateBytes, "write", s.WriteRateBps)
		setAllMetric(o.AllRateIops, "read", s.ReadIops)
		setAllMetric(o.AllRateIops, "write", s.WriteIops)
	}

	return nil
}

func (o *IOShapingCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

func (o *IOShapingCollector) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range o.collectorList() {
		if gaugeVec, ok := metric.(*prometheus.GaugeVec); ok {
			gaugeVec.Reset()
		}
	}

	if err := o.collectIOShaping(); err != nil {
		log.Println("failed collecting IO shaping metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
