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

	// Grouped IO Rates
	ReadRate  *prometheus.GaugeVec
	WriteRate *prometheus.GaugeVec
	ReadIops  *prometheus.GaugeVec
	WriteIops *prometheus.GaugeVec

	SystemLoopDurationUs   *prometheus.GaugeVec
	SystemReportsProcessed *prometheus.GaugeVec
}

func NewIOShapingCollector(opts *CollectorOpts) *IOShapingCollector {
	cluster := opts.Cluster
	labels := prometheus.Labels{"cluster": cluster}
	namespace := "eos"

	standardLabels := []string{"type", "id", "window_sec"}
	systemLabels := []string{"loop_name", "stat"}
	reportLabels := []string{"stat"}

	return &IOShapingCollector{
		CollectorOpts: opts,
		// Standard IO Rates
		ReadRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_read_rate_bytes", Help: "Read rate in bytes per second", ConstLabels: labels,
		}, standardLabels),
		WriteRate: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_write_rate_bytes", Help: "Write rate in bytes per second", ConstLabels: labels,
		}, standardLabels),
		ReadIops: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_read_iops", Help: "Read IOPS", ConstLabels: labels,
		}, standardLabels),
		WriteIops: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_write_iops", Help: "Write IOPS", ConstLabels: labels,
		}, standardLabels),

		SystemLoopDurationUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_sys_loop_duration_microseconds", Help: "System thread loop duration in microseconds", ConstLabels: labels,
		}, systemLabels),
		SystemReportsProcessed: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_reports_processed_per_sec", Help: "Number of FST IO reports processed by the shaping engine per tick", ConstLabels: labels,
		}, reportLabels),
	}
}

func (o *IOShapingCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.ReadRate, o.WriteRate, o.ReadIops, o.WriteIops, o.SystemLoopDurationUs, o.SystemReportsProcessed,
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

	windows := []int{15, 60, 300}

	for _, win := range windows {
		stats, err := client.ListIOShaping(context.Background(), win)
		if err != nil {
			log.Printf("failed to collect IO shaping for window %ds: %v", win, err)
			continue
		}

		for _, s := range stats {
			if s.Type == "system" {
				setSysDurationMetric := func(loopName, statName, valStr string) {
					if valStr == "" {
						return
					}
					if val, err := strconv.ParseFloat(valStr, 64); err == nil {
						o.SystemLoopDurationUs.WithLabelValues(loopName, statName).Set(val)
					}
				}

				setSysCountMetric := func(statName, valStr string) {
					if valStr == "" {
						return
					}
					if val, err := strconv.ParseFloat(valStr, 64); err == nil {
						o.SystemReportsProcessed.WithLabelValues(statName).Set(val)
					}
				}

				setSysDurationMetric("estimators", "median", s.EstimatorsLoopMedianUs)
				setSysDurationMetric("estimators", "min", s.EstimatorsLoopMinUs)
				setSysDurationMetric("estimators", "max", s.EstimatorsLoopMaxUs)

				setSysDurationMetric("fst_limits", "median", s.FstLimitsLoopMedianUs)
				setSysDurationMetric("fst_limits", "min", s.FstLimitsLoopMinUs)
				setSysDurationMetric("fst_limits", "max", s.FstLimitsLoopMaxUs)

				setSysCountMetric("mean", s.FstReportsProcessedPerSecMean)

				continue
			}

			setMetric := func(vec *prometheus.GaugeVec, valStr string) {
				if val, err := strconv.ParseFloat(valStr, 64); err == nil {
					vec.WithLabelValues(s.Type, s.ID, s.WindowSec).Set(val)
				}
			}

			setMetric(o.ReadRate, s.ReadRateBps)
			setMetric(o.WriteRate, s.WriteRateBps)
			setMetric(o.ReadIops, s.ReadIops)
			setMetric(o.WriteIops, s.WriteIops)
		}
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
