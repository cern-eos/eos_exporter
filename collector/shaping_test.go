package collector

import (
	"bytes"
	"context"
	"errors"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func counterCollector(snapshot *eosclient.IOShapingCounters) *IOShapingCollector {
	o := NewIOShapingCollector(&CollectorOpts{Cluster: "test"})
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) { return snapshot, nil }
	o.userLabel = func(id string) string { return resolvedShapingID(id, "alice") }
	o.groupLabel = func(id string) string { return resolvedShapingID(id, "users") }
	return o
}

func gatherCounters(t *testing.T, o *IOShapingCollector) map[string]*dto.MetricFamily {
	t.Helper()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(o)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	result := make(map[string]*dto.MetricFamily)
	for _, family := range families {
		result[family.GetName()] = family
	}
	return result
}

func findMetric(t *testing.T, families map[string]*dto.MetricFamily, name string, labels map[string]string) *dto.Metric {
	t.Helper()
	family := families[name]
	if family == nil {
		t.Fatalf("missing family %s", name)
	}
	for _, m := range family.Metric {
		matched := 0
		for _, label := range m.Label {
			if value, ok := labels[label.GetName()]; ok && value == label.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return m
		}
	}
	t.Fatalf("missing %s %v", name, labels)
	return nil
}

func TestShapingNativeCounterContractAndProjections(t *testing.T) {
	snapshot := &eosclient.IOShapingCounters{Version: 1, LimitEntries: 50000, Entries: []eosclient.IOShapingCounter{
		{NodeID: "/eos/node1:1095/fst", App: "analysis", UID: 100, GID: 200, BytesReadTotal: 100, BytesWrittenTotal: 200, ReadOpsTotal: 3, WriteOpsTotal: 4},
		{NodeID: "node2:1095", App: "analysis", UID: 100, GID: 200, BytesReadTotal: 50, BytesWrittenTotal: 60, ReadOpsTotal: 5, WriteOpsTotal: 6},
	}}
	families := gatherCounters(t, counterCollector(snapshot))
	for _, name := range []string{"eos_io_shaping_bytes_total", "eos_io_shaping_operations_total", "eos_io_shaping_all_bytes_total", "eos_io_shaping_all_operations_total"} {
		if families[name].GetType() != dto.MetricType_COUNTER {
			t.Fatalf("%s is not a counter", name)
		}
	}
	for kind, id := range map[string]string{"app": "analysis", "uid": "100(alice)", "gid": "200(users)"} {
		m := findMetric(t, families, "eos_io_shaping_bytes_total", map[string]string{"cluster": "test", "type": kind, "id": id, "operation": "read"})
		if m.GetCounter().GetValue() != 150 {
			t.Fatalf("%s total=%v", kind, m)
		}
	}
	m := findMetric(t, families, "eos_io_shaping_all_bytes_total", map[string]string{"node_id": "node1:1095", "fsid": "0", "app": "analysis", "uid": "100(alice)", "uid_id": "100", "uid_name": "100(alice)", "gid": "200(users)", "gid_id": "200", "gid_name": "200(users)", "groups": "200(users)", "operation": "read"})
	if len(m.Label) != 12 || m.GetCounter().GetValue() != 100 {
		t.Fatalf("native all-tags contract mismatch: %v", m)
	}
	if families["eos_io_shaping_fs_bytes_total"] != nil {
		t.Fatal("invented filesystem counters")
	}
	if families["eos_io_shaping_rate_bytes"] != nil {
		t.Fatal("unexpected legacy rate family")
	}
	findMetric(t, families, "eos_monit_enabled", map[string]string{"cluster": "test"})
}

func TestShapingNormalizesCollidingIdentitiesWithoutDuplicateSamples(t *testing.T) {
	snapshot := &eosclient.IOShapingCounters{Entries: []eosclient.IOShapingCounter{
		{NodeID: "/eos/node:1095/fst", App: "", BytesReadTotal: 10},
		{NodeID: "node:1095", App: "<unknown>", BytesReadTotal: 20},
	}}
	families := gatherCounters(t, counterCollector(snapshot))
	m := findMetric(t, families, "eos_io_shaping_all_bytes_total", map[string]string{"node_id": "node:1095", "app": "<unknown>", "operation": "read"})
	if m.GetCounter().GetValue() != 30 {
		t.Fatal(m)
	}
}

func TestShapingResetDisappearanceAndFailureDoNotInventCounters(t *testing.T) {
	o := counterCollector(&eosclient.IOShapingCounters{Entries: []eosclient.IOShapingCounter{{App: "app", BytesReadTotal: 100}}})
	gatherCounters(t, o)
	o.lastRefresh = time.Time{}
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return &eosclient.IOShapingCounters{Entries: []eosclient.IOShapingCounter{{App: "app", BytesReadTotal: 5}}}, nil
	}
	families := gatherCounters(t, o)
	if m := findMetric(t, families, "eos_io_shaping_bytes_total", map[string]string{"type": "app", "operation": "read"}); m.GetCounter().GetValue() != 5 {
		t.Fatal(m)
	}
	o.lastRefresh = time.Time{}
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) { return nil, errors.New("MGM unavailable") }
	families = gatherCounters(t, o)
	if families["eos_io_shaping_bytes_total"] != nil {
		t.Fatal("stale counters on failed scrape")
	}
	if m := findMetric(t, families, "eos_io_shaping_scrape_success", nil); m.GetGauge().GetValue() != 0 {
		t.Fatal(m)
	}
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return &eosclient.IOShapingCounters{Entries: []eosclient.IOShapingCounter{}}, nil
	}
	families = gatherCounters(t, o)
	if families["eos_io_shaping_bytes_total"] != nil {
		t.Fatal("disappeared series retained")
	}
}

func TestShapingRepeatedFailuresLogOnce(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)

	o := counterCollector(nil)
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return nil, errors.New("unsupported EOS")
	}
	gatherCounters(t, o)
	gatherCounters(t, o)
	if count := strings.Count(logs.String(), "failed collecting IO shaping counters"); count != 1 {
		t.Fatalf("logged repeated failure %d times: %s", count, logs.String())
	}

	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return &eosclient.IOShapingCounters{Entries: []eosclient.IOShapingCounter{}}, nil
	}
	gatherCounters(t, o)
	o.lastRefresh = time.Time{}
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		return nil, errors.New("EOS unavailable again")
	}
	gatherCounters(t, o)
	if count := strings.Count(logs.String(), "failed collecting IO shaping counters"); count != 2 {
		t.Fatalf("did not log new failure period: %s", logs.String())
	}
}

func TestShapingConcurrentScrapesShareSuccessfulSnapshot(t *testing.T) {
	o := counterCollector(&eosclient.IOShapingCounters{})
	calls := 0
	o.fetch = func(context.Context) (*eosclient.IOShapingCounters, error) {
		calls++
		return &eosclient.IOShapingCounters{}, nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); gatherCounters(t, o) }()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("%d command calls", calls)
	}
}

func TestShapingAdmissionLossIsVisible(t *testing.T) {
	families := gatherCounters(t, counterCollector(&eosclient.IOShapingCounters{RejectedEntriesTotal: 7, LimitEntries: 50000}))
	if m := findMetric(t, families, "eos_io_shaping_all_entries_limited", nil); m.GetGauge().GetValue() != 1 {
		t.Fatal(m)
	}
	if m := findMetric(t, families, "eos_io_shaping_counter_entries_rejected_total", nil); m.GetCounter().GetValue() != 7 {
		t.Fatal(m)
	}
}

func TestShapingLabelsMatchNativeResolvedIDConvention(t *testing.T) {
	for _, test := range []struct{ id, name, want string }{{"100", "alice", "100(alice)"}, {"100", "100", "100"}, {"100", "", "100"}} {
		if got := resolvedShapingID(test.id, test.name); got != test.want {
			t.Fatalf("got %s, want %s", got, test.want)
		}
	}
	if strings.Contains(shapingNodeLabel("/eos/node:1095/fst"), "/eos/") {
		t.Fatal("node prefix retained")
	}
}

// Fixture generated by the EOS 5.4 IoCounters implementation after two nodes
// report cumulative observations. Exercise CLI decoding through metric emission.
func TestShapingEOS54SnapshotThroughClientAndCollector(t *testing.T) {
	data, err := os.ReadFile("../testdata/eos54-counters.json")
	if err != nil {
		t.Fatal(err)
	}
	binary := filepath.Join(t.TempDir(), "eos")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\ncat <<'JSON'\n"+string(data)+"JSON\n"), 0700); err != nil {
		t.Fatal(err)
	}
	client, err := eosclient.New(&eosclient.Options{EosBinary: binary, Timeout: 2})
	if err != nil {
		t.Fatal(err)
	}
	o := counterCollector(nil)
	o.fetch = client.ListIOShapingCounters
	families := gatherCounters(t, o)
	metric := findMetric(t, families, "eos_io_shaping_bytes_total", map[string]string{"type": "app", "id": "analysis", "operation": "read"})
	if metric.GetCounter().GetValue() != 450 {
		t.Fatal(metric)
	}
	metric = findMetric(t, families, "eos_io_shaping_all_operations_total", map[string]string{"node_id": "node2:1095", "operation": "write"})
	if metric.GetCounter().GetValue() != 60 {
		t.Fatal(metric)
	}
}
