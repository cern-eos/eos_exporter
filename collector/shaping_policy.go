package collector

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

type IOShapingPolicyCollector struct {
	*CollectorOpts

	// Single grouped metric for all policy limits and reservations
	PolicyBytes *prometheus.GaugeVec
}

func NewIOShapingPolicyCollector(opts *CollectorOpts) *IOShapingPolicyCollector {
	cluster := opts.Cluster
	labels := prometheus.Labels{"cluster": cluster}
	namespace := "eos"

	// "rule" distinguishes between limit_read, limit_write, etc.
	standardLabels := []string{"type", "id", "rule"}

	return &IOShapingPolicyCollector{
		CollectorOpts: opts,
		PolicyBytes: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_policy_bytes",
			Help:        "Configured limits and reservations in bytes per second (0 if policy is disabled)",
			ConstLabels: labels,
		}, standardLabels),
	}
}

func (o *IOShapingPolicyCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.PolicyBytes,
	}
}

func (o *IOShapingPolicyCollector) collectIOShapingPolicies() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		return fmt.Errorf("failed to create eosclient: %w", err)
	}

	policies, err := client.ListIOShapingPolicies(context.Background())
	if err != nil {
		return fmt.Errorf("failed to collect IO shaping policies: %w", err)
	}

	for _, p := range policies {
		// Helper to set the metric: uses the actual value if enabled, otherwise 0
		setMetric := func(ruleName string, valStr string) {
			valToSet := 0.0

			if p.IsEnabled && valStr != "" {
				if parsedVal, err := strconv.ParseFloat(valStr, 64); err == nil {
					valToSet = parsedVal
				}
			}

			o.PolicyBytes.WithLabelValues(p.Type, p.ID, ruleName).Set(valToSet)
		}

		setMetric("limit_read", p.LimitReadBytes)
		setMetric("limit_write", p.LimitWriteBytes)
		setMetric("reservation_read", p.ReservationReadBytes)
		setMetric("reservation_write", p.ReservationWriteBytes)
	}

	return nil
}

func (o *IOShapingPolicyCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

func (o *IOShapingPolicyCollector) Collect(ch chan<- prometheus.Metric) {
	// Reset the GaugeVec before scrape
	for _, metric := range o.collectorList() {
		if gaugeVec, ok := metric.(*prometheus.GaugeVec); ok {
			gaugeVec.Reset()
		}
	}

	if err := o.collectIOShapingPolicies(); err != nil {
		log.Println("failed collecting IO shaping policy metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
