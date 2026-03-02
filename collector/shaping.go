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

	// System loop metrics (Grouped into one metric)
	SystemLoopDurationUs *prometheus.GaugeVec
}

func NewIOShapingCollector(opts *CollectorOpts) *IOShapingCollector {
	cluster := opts.Cluster
	labels := prometheus.Labels{"cluster": cluster}
	namespace := "eos"

	standardLabels := []string{"type", "id", "window_sec"}
	systemLabels := []string{"loop_name", "stat"} // e.g., loop_name="estimators", stat="mean"

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

		// Single grouped System Metric
		SystemLoopDurationUs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace: namespace, Name: "io_shaping_sys_loop_duration_microseconds", Help: "System thread loop duration in microseconds", ConstLabels: labels,
		}, systemLabels),
	}
}

func (o *IOShapingCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.ReadRate, o.WriteRate, o.ReadIops, o.WriteIops, o.SystemLoopDurationUs,
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

				setSysMetric("estimators", "mean", s.EstimatorsLoopMeanUs)
				setSysMetric("estimators", "min", s.EstimatorsLoopMinUs)
				setSysMetric("estimators", "max", s.EstimatorsLoopMaxUs)

				setSysMetric("fst_limits", "mean", s.FstLimitsLoopMeanUs)
				setSysMetric("fst_limits", "min", s.FstLimitsLoopMinUs)
				setSysMetric("fst_limits", "max", s.FstLimitsLoopMaxUs)

				continue // Skip the standard groupings for this system JSON object
			}

			// Handle Standard Groupings
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
	// Reset all GaugeVecs
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
