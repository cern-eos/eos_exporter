package collector

import (
	"context"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

const shapingCacheTTL = time.Second

type IOShapingCollector struct {
	*CollectorOpts
	mu            sync.Mutex
	lastRefresh   time.Time
	snapshot      *eosclient.IOShapingCounters
	failureLogged bool
	fetch         func(context.Context) (*eosclient.IOShapingCounters, error)
	userLabel     func(string) string
	groupLabel    func(string) string
	desc          map[string]*prometheus.Desc
}

func NewIOShapingCollector(opts *CollectorOpts) *IOShapingCollector {
	resolver := newUnixIDResolver()
	o := &IOShapingCollector{CollectorOpts: opts, desc: make(map[string]*prometheus.Desc)}
	o.userLabel = func(id string) string { return resolvedShapingID(id, resolver.ResolveUser(id)) }
	o.groupLabel = func(id string) string { return resolvedShapingID(id, resolver.ResolveGroup(id)) }
	o.fetch = func(ctx context.Context) (*eosclient.IOShapingCounters, error) {
		client, err := eosclient.New(&eosclient.Options{URL: "root://" + getEOSInstance(), Timeout: opts.Timeout})
		if err != nil {
			return nil, err
		}
		return client.ListIOShapingCounters(ctx)
	}
	add := func(name, help string, labels ...string) {
		o.desc[name] = prometheus.NewDesc("eos_"+name, help, labels, prometheus.Labels{"cluster": opts.Cluster})
	}
	add("io_shaping_bytes_total", "Total IO shaping bytes observed", "type", "id", "operation")
	add("io_shaping_operations_total", "Total IO shaping operations observed", "type", "id", "operation")
	labels := []string{"node_id", "fsid", "app", "uid", "uid_id", "uid_name", "gid", "gid_id", "gid_name", "groups", "operation"}
	add("io_shaping_all_bytes_total", "Total IO shaping all-tags bytes observed", labels...)
	add("io_shaping_all_operations_total", "Total IO shaping all-tags operations observed", labels...)
	add("io_shaping_all_entries", "Number of retained all-tags IO shaping entries.")
	add("io_shaping_all_entries_exported", "Number of all-tags IO shaping entries exported in this scrape.")
	add("io_shaping_all_entries_limited", "Whether the MGM has rejected new monitoring identities.")
	add("io_shaping_counter_entries_rejected_total", "Monitoring identities rejected by MGM retention bounds.")
	add("io_shaping_counter_entries_limit", "Maximum retained monitoring identities.")
	add("io_shaping_scrape_success", "Whether cumulative shaping counters were collected successfully.")
	add("io_shaping_sys_loop_duration_microseconds", "System thread loop duration in microseconds", "loop_name", "stat")
	add("io_shaping_reports_processed_per_sec", "FST IO reports processed per second", "stat")
	add("monit_enabled", "Whether this exporter provides the EOS monitoring metric interface.")
	add("monit_cache_ttl_seconds", "Counter snapshot cache lifetime in seconds.")
	return o
}

func resolvedShapingID(id, name string) string {
	if name == "" || name == id {
		return id
	}
	return id + "(" + name + ")"
}

func shapingNodeLabel(node string) string {
	if strings.HasPrefix(node, "/eos/") && strings.HasSuffix(node, "/fst") {
		node = strings.TrimSuffix(strings.TrimPrefix(node, "/eos/"), "/fst")
	}
	if node == "" {
		return "<unknown>"
	}
	return node
}

func (o *IOShapingCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range o.desc {
		ch <- desc
	}
}

func (o *IOShapingCollector) Collect(ch chan<- prometheus.Metric) {
	// Serialize refreshes and label resolution; concurrent HTTP scrapes must not
	// race a mutable snapshot or run duplicate EOS commands.
	o.mu.Lock()
	defer o.mu.Unlock()
	emit := func(name string, kind prometheus.ValueType, value float64, labels ...string) {
		ch <- prometheus.MustNewConstMetric(o.desc[name], kind, value, labels...)
	}
	emit("monit_enabled", prometheus.GaugeValue, 1)
	emit("monit_cache_ttl_seconds", prometheus.GaugeValue, shapingCacheTTL.Seconds())
	if o.snapshot == nil || time.Since(o.lastRefresh) >= shapingCacheTTL {
		snapshot, err := o.fetch(context.Background())
		if err != nil {
			// Do not keep exposing an old snapshot as healthy after an EOS failure.
			o.snapshot = nil
			if !o.failureLogged {
				log.Printf("failed collecting IO shaping counters: %v", err)
				o.failureLogged = true
			}
			emit("io_shaping_scrape_success", prometheus.GaugeValue, 0)
			return
		}
		o.failureLogged = false
		o.snapshot = snapshot
		o.lastRefresh = time.Now()
	}
	emit("io_shaping_scrape_success", prometheus.GaugeValue, 1)
	snapshot := o.snapshot
	emit("io_shaping_all_entries", prometheus.GaugeValue, float64(len(snapshot.Entries)))
	emit("io_shaping_all_entries_exported", prometheus.GaugeValue, float64(len(snapshot.Entries)))
	limited := 0.0
	if snapshot.RejectedEntriesTotal > 0 {
		limited = 1
	}
	emit("io_shaping_all_entries_limited", prometheus.GaugeValue, limited)
	emit("io_shaping_counter_entries_rejected_total", prometheus.CounterValue, float64(snapshot.RejectedEntriesTotal))
	emit("io_shaping_counter_entries_limit", prometheus.GaugeValue, float64(snapshot.LimitEntries))

	type projection struct{ kind, id string }
	projections := make(map[projection][4]float64)
	// Match the native exporter label formatting, and aggregate identities that
	// normalize to the same node/application labels before emitting samples.
	type identity struct {
		node, app string
		uid, gid  uint32
	}
	all := make(map[identity][4]float64)
	for _, row := range snapshot.Entries {
		app := row.App
		if app == "" {
			app = "<unknown>"
		}
		key := identity{shapingNodeLabel(row.NodeID), app, row.UID, row.GID}
		values := all[key]
		for i, v := range []uint64{row.BytesReadTotal, row.BytesWrittenTotal, row.ReadOpsTotal, row.WriteOpsTotal} {
			values[i] += float64(v)
		}
		all[key] = values
	}
	for key, values := range all {
		uidID, gidID := strconv.FormatUint(uint64(key.uid), 10), strconv.FormatUint(uint64(key.gid), 10)
		uid, gid := o.userLabel(uidID), o.groupLabel(gidID)
		for _, p := range []projection{{"app", key.app}, {"uid", uid}, {"gid", gid}, {"node", key.node}} {
			total := projections[p]
			for i, v := range values {
				total[i] += v
			}
			projections[p] = total
		}
		for i, operation := range []string{"read", "write"} {
			// fsid=0 is the native exporter's aggregate/unknown-filesystem convention.
			labels := []string{key.node, "0", key.app, uid, uidID, uid, gid, gidID, gid, gid, operation}
			emit("io_shaping_all_bytes_total", prometheus.CounterValue, values[i], labels...)
			emit("io_shaping_all_operations_total", prometheus.CounterValue, values[i+2], labels...)
		}
	}
	for key, values := range projections {
		for i, operation := range []string{"read", "write"} {
			emit("io_shaping_bytes_total", prometheus.CounterValue, values[i], key.kind, key.id, operation)
			emit("io_shaping_operations_total", prometheus.CounterValue, values[i+2], key.kind, key.id, operation)
		}
	}
	system := snapshot.System
	for _, loop := range []struct {
		name   string
		values [3]string
	}{
		{"estimators", [3]string{system.EstimatorsLoopMedianUs.String(), system.EstimatorsLoopMinUs.String(), system.EstimatorsLoopMaxUs.String()}},
		{"fst_limits", [3]string{system.FstLimitsLoopMedianUs.String(), system.FstLimitsLoopMinUs.String(), system.FstLimitsLoopMaxUs.String()}},
	} {
		for i, stat := range []string{"median", "min", "max"} {
			if loop.values[i] == "" {
				continue
			}
			value, err := strconv.ParseFloat(loop.values[i], 64)
			if err == nil {
				emit("io_shaping_sys_loop_duration_microseconds", prometheus.GaugeValue, value, loop.name, stat)
			}
		}
	}
	if value, err := strconv.ParseFloat(system.ReportsProcessedPerSecMean.String(), 64); err == nil {
		emit("io_shaping_reports_processed_per_sec", prometheus.GaugeValue, value, "mean")
	}
}

var _ prometheus.Collector = (*IOShapingCollector)(nil)
