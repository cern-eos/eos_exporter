package collector

import (
	"context"
	"fmt"
	"log"
	"strconv"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

type shapingRateValues struct {
	ReadRateBps  float64
	WriteRateBps float64
	ReadIops     float64
	WriteIops    float64
}

type shapingStandardKey struct {
	Type      string
	ID        string
	WindowSec string
}

type shapingFSKey struct {
	NodeID    string
	FSID      string
	WindowSec string
}

type IOShapingCollector struct {
	*CollectorOpts
	idResolver *unixIDResolver

	RateBytes *prometheus.GaugeVec
	RateIops  *prometheus.GaugeVec

	FSRateBytes *prometheus.GaugeVec
	FSRateIops  *prometheus.GaugeVec

	AllRateBytes *prometheus.GaugeVec
	AllRateIops  *prometheus.GaugeVec
	AllEntries   *prometheus.GaugeVec

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
	allLabels := []string{"node_id", "fsid", "app", "uid", "uid_name", "gid", "gid_name", "window_sec", "operation"}
	systemLabels := []string{"loop_name", "stat"}
	reportLabels := []string{"stat"}

	return &IOShapingCollector{
		CollectorOpts: opts,
		idResolver:    newUnixIDResolver(),

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

		AllEntries: prometheus.NewGaugeVec(prometheus.GaugeOpts{
			Namespace:   namespace,
			Name:        "io_shaping_all_entries",
			Help:        "Number of entries returned by eos io shaping ls --all --json for the configured window.",
			ConstLabels: labels,
		}, []string{"window_sec"}),

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
		o.RateBytes, o.RateIops, o.FSRateBytes, o.FSRateIops, o.AllRateBytes, o.AllRateIops, o.AllEntries, o.SystemLoopDurationUs, o.ReportsProcessedPerSec,
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
	var allStats []*eosclient.IOShapingAllStat

	for _, win := range windows {
		stats, err := client.ListIOShapingAll(context.Background(), win)
		if err != nil {
			log.Printf("failed to collect IO shaping all-tags stats for window %ds: %v", win, err)
			continue
		}
		o.AllEntries.WithLabelValues(strconv.Itoa(win)).Set(float64(countIOShapingAllEntries(stats)))
		allStats = append(allStats, stats...)
	}

	if len(allStats) == 0 {
		return nil
	}

	standardStats, fsStats, _ := projectIOShapingAll(allStats)

	for key, values := range standardStats {
		setProjectedMetric(o.RateBytes, key.Type, key.ID, key.WindowSec, "read", values.ReadRateBps)
		setProjectedMetric(o.RateBytes, key.Type, key.ID, key.WindowSec, "write", values.WriteRateBps)
		setProjectedMetric(o.RateIops, key.Type, key.ID, key.WindowSec, "read", values.ReadIops)
		setProjectedMetric(o.RateIops, key.Type, key.ID, key.WindowSec, "write", values.WriteIops)
	}

	for key, values := range fsStats {
		setFSProjectedMetric(o.FSRateBytes, key.NodeID, key.FSID, key.WindowSec, "read", values.ReadRateBps)
		setFSProjectedMetric(o.FSRateBytes, key.NodeID, key.FSID, key.WindowSec, "write", values.WriteRateBps)
		setFSProjectedMetric(o.FSRateIops, key.NodeID, key.FSID, key.WindowSec, "read", values.ReadIops)
		setFSProjectedMetric(o.FSRateIops, key.NodeID, key.FSID, key.WindowSec, "write", values.WriteIops)
	}

	for _, s := range allStats {
		if s.Type == "system" {
			o.collectSystemMetrics(s)
			continue
		}

		uidName := o.idResolver.ResolveUser(s.UID)
		gidName := o.idResolver.ResolveGroup(s.GID)

		setAllMetric := func(vec *prometheus.GaugeVec, operation, valStr string) {
			if valStr == "" {
				return
			}
			if val, err := strconv.ParseFloat(valStr, 64); err == nil {
				vec.WithLabelValues(s.NodeID, s.FSID, s.App, s.UID, uidName, s.GID, gidName, s.WindowSec, operation).Set(val)
			}
		}

		setAllMetric(o.AllRateBytes, "read", s.ReadRateBps)
		setAllMetric(o.AllRateBytes, "write", s.WriteRateBps)
		setAllMetric(o.AllRateIops, "read", s.ReadIops)
		setAllMetric(o.AllRateIops, "write", s.WriteIops)
	}

	return nil
}

func countIOShapingAllEntries(stats []*eosclient.IOShapingAllStat) int {
	entries := 0
	for _, s := range stats {
		if s.Type == "all" {
			entries++
		}
	}
	return entries
}

func (o *IOShapingCollector) collectSystemMetrics(s *eosclient.IOShapingAllStat) {
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

	if s.ReportsProcessedPerSecMean != "" {
		if val, err := strconv.ParseFloat(s.ReportsProcessedPerSecMean, 64); err == nil {
			o.ReportsProcessedPerSec.WithLabelValues("mean").Set(val)
		}
	}
}

func projectIOShapingAll(stats []*eosclient.IOShapingAllStat) (map[shapingStandardKey]shapingRateValues, map[shapingFSKey]shapingRateValues, int) {
	standardStats := make(map[shapingStandardKey]shapingRateValues)
	fsStats := make(map[shapingFSKey]shapingRateValues)
	allEntries := 0

	for _, s := range stats {
		if s.Type != "all" {
			continue
		}

		allEntries++
		values := shapingRateValues{
			ReadRateBps:  parseShapingFloat(s.ReadRateBps),
			WriteRateBps: parseShapingFloat(s.WriteRateBps),
			ReadIops:     parseShapingFloat(s.ReadIops),
			WriteIops:    parseShapingFloat(s.WriteIops),
		}

		addStandardProjection(standardStats, shapingStandardKey{Type: "app", ID: s.App, WindowSec: s.WindowSec}, values)
		addStandardProjection(standardStats, shapingStandardKey{Type: "uid", ID: s.UID, WindowSec: s.WindowSec}, values)
		addStandardProjection(standardStats, shapingStandardKey{Type: "gid", ID: s.GID, WindowSec: s.WindowSec}, values)
		addStandardProjection(standardStats, shapingStandardKey{Type: "node", ID: s.NodeID, WindowSec: s.WindowSec}, values)

		addFSProjection(fsStats, shapingFSKey{NodeID: s.NodeID, FSID: s.FSID, WindowSec: s.WindowSec}, values)
	}

	return standardStats, fsStats, allEntries
}

func addStandardProjection(stats map[shapingStandardKey]shapingRateValues, key shapingStandardKey, values shapingRateValues) {
	if key.ID == "" || key.WindowSec == "" {
		return
	}
	stats[key] = addShapingRateValues(stats[key], values)
}

func addFSProjection(stats map[shapingFSKey]shapingRateValues, key shapingFSKey, values shapingRateValues) {
	if key.NodeID == "" || key.FSID == "" || key.WindowSec == "" {
		return
	}
	stats[key] = addShapingRateValues(stats[key], values)
}

func addShapingRateValues(a, b shapingRateValues) shapingRateValues {
	return shapingRateValues{
		ReadRateBps:  a.ReadRateBps + b.ReadRateBps,
		WriteRateBps: a.WriteRateBps + b.WriteRateBps,
		ReadIops:     a.ReadIops + b.ReadIops,
		WriteIops:    a.WriteIops + b.WriteIops,
	}
}

func parseShapingFloat(valStr string) float64 {
	if valStr == "" {
		return 0
	}
	val, err := strconv.ParseFloat(valStr, 64)
	if err != nil {
		return 0
	}
	return val
}

func setProjectedMetric(vec *prometheus.GaugeVec, statType, id, windowSec, operation string, val float64) {
	vec.WithLabelValues(statType, id, windowSec, operation).Set(val)
}

func setFSProjectedMetric(vec *prometheus.GaugeVec, nodeID, fsid, windowSec, operation string, val float64) {
	vec.WithLabelValues(nodeID, fsid, windowSec, operation).Set(val)
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
