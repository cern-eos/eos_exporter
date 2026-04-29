package collector

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

const ioShapingConfigRefreshInterval = 5 * time.Minute

type IOShapingConfigCollector struct {
	*CollectorOpts

	mu          sync.Mutex
	lastRefresh time.Time
	config      *eosclient.IOShapingConfig

	Enabled                     *prometheus.GaugeVec
	EstimatorsUpdatePeriodMs    *prometheus.GaugeVec
	FstIOPolicyUpdatePeriodMs   *prometheus.GaugeVec
	FstIOStatsReportingPeriodMs *prometheus.GaugeVec
	DetailFilesystem            *prometheus.GaugeVec
	SystemStatsTimeWindowSec    *prometheus.GaugeVec
}

func NewIOShapingConfigCollector(opts *CollectorOpts) *IOShapingConfigCollector {
	cluster := opts.Cluster
	labels := prometheus.Labels{"cluster": cluster}
	namespace := "eos"

	return &IOShapingConfigCollector{
		CollectorOpts: opts,

		Enabled: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_enabled",
			Help:        "Traffic shaping configuration status (1 if enabled, 0 if disabled).",
			ConstLabels: labels,
		}, []string{}),

		EstimatorsUpdatePeriodMs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_estimators_update_period_milliseconds",
			Help:        "Configured IO shaping estimators update period in milliseconds.",
			ConstLabels: labels,
		}, []string{}),

		FstIOPolicyUpdatePeriodMs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_fst_io_policy_update_period_milliseconds",
			Help:        "Configured FST IO policy update period in milliseconds.",
			ConstLabels: labels,
		}, []string{}),

		FstIOStatsReportingPeriodMs: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_fst_io_stats_reporting_period_milliseconds",
			Help:        "Configured FST IO stats reporting period in milliseconds.",
			ConstLabels: labels,
		}, []string{}),

		DetailFilesystem: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_detail_filesystem",
			Help:        "Traffic shaping stats detail level (1 if filesystem, 0 otherwise).",
			ConstLabels: labels,
		}, []string{}),

		SystemStatsTimeWindowSec: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_config_system_stats_time_window_seconds",
			Help:        "Configured IO shaping system stats time window in seconds.",
			ConstLabels: labels,
		}, []string{}),
	}
}

func (o *IOShapingConfigCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Enabled,
		o.EstimatorsUpdatePeriodMs,
		o.FstIOPolicyUpdatePeriodMs,
		o.FstIOStatsReportingPeriodMs,
		o.DetailFilesystem,
		o.SystemStatsTimeWindowSec,
	}
}

func (o *IOShapingConfigCollector) fetchIOShapingConfig() (*eosclient.IOShapingConfig, error) {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		return nil, fmt.Errorf("failed to create eosclient: %w", err)
	}

	config, err := client.ListIOShapingConfig(context.Background())
	if err != nil {
		return nil, fmt.Errorf("failed to collect IO shaping config: %w", err)
	}

	return config, nil
}

func (o *IOShapingConfigCollector) configForScrape() (*eosclient.IOShapingConfig, error) {
	o.mu.Lock()
	defer o.mu.Unlock()

	if o.config != nil && time.Since(o.lastRefresh) < ioShapingConfigRefreshInterval {
		return o.config, nil
	}

	config, err := o.fetchIOShapingConfig()
	if err != nil {
		if o.config != nil {
			log.Println("failed refreshing IO shaping config metrics, using cached values:", err)
			return o.config, nil
		}
		return nil, err
	}

	o.config = config
	o.lastRefresh = time.Now()
	return o.config, nil
}

func setShapingConfigGaugeFromString(vec *prometheus.GaugeVec, valStr string) {
	if valStr == "" {
		return
	}
	if val, err := strconv.ParseFloat(valStr, 64); err == nil {
		vec.WithLabelValues().Set(val)
	}
}

func (o *IOShapingConfigCollector) collectIOShapingConfig() error {
	config, err := o.configForScrape()
	if err != nil {
		return err
	}

	if config.Enabled {
		o.Enabled.WithLabelValues().Set(1)
	} else {
		o.Enabled.WithLabelValues().Set(0)
	}

	setShapingConfigGaugeFromString(o.EstimatorsUpdatePeriodMs, config.EstimatorsUpdatePeriodMs)
	setShapingConfigGaugeFromString(o.FstIOPolicyUpdatePeriodMs, config.FstIOPolicyUpdatePeriodMs)
	setShapingConfigGaugeFromString(o.FstIOStatsReportingPeriodMs, config.FstIOStatsReportingPeriodMs)

	if config.DetailFilesystem {
		o.DetailFilesystem.WithLabelValues().Set(1)
	} else {
		o.DetailFilesystem.WithLabelValues().Set(0)
	}

	setShapingConfigGaugeFromString(o.SystemStatsTimeWindowSec, config.SystemStatsTimeWindowSeconds)

	return nil
}

func (o *IOShapingConfigCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

func (o *IOShapingConfigCollector) Collect(ch chan<- prometheus.Metric) {
	for _, metric := range o.collectorList() {
		if gaugeVec, ok := metric.(*prometheus.GaugeVec); ok {
			gaugeVec.Reset()
		}
	}

	if err := o.collectIOShapingConfig(); err != nil {
		log.Println("failed collecting IO shaping config metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
