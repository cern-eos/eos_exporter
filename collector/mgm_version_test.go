package collector

import (
	"bytes"
	"context"
	"errors"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

func TestMGMLeaderVersionCollector(t *testing.T) {
	o := NewMGMLeaderVersionCollector(&CollectorOpts{Cluster: "eospilot"})
	calls := 0
	o.fetch = func(context.Context) (*eosclient.MGMLeaderVersion, error) {
		calls++
		return &eosclient.MGMLeaderVersion{Leader: "eospilot-ns-02.cern.ch:1094", Version: "5.5.2", Release: "1"}, nil
	}
	o.refresh()
	o.lastRefresh = time.Now()
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(o)
	for range 2 {
		families, err := registry.Gather()
		if err != nil {
			t.Fatal(err)
		}
		byName := make(map[string]*dto.MetricFamily)
		for _, family := range families {
			byName[family.GetName()] = family
		}
		m := findMetric(t, byName, "eos_mgm_leader_info", map[string]string{
			"cluster": "eospilot", "mgm": "eospilot-ns-02.cern.ch:1094", "eos_version": "5.5.2", "eos_release": "1",
		})
		if m.GetGauge().GetValue() != 1 {
			t.Fatalf("leader info=%v", m)
		}
	}
	if calls != 1 {
		t.Fatalf("expected cached version, got %d fetches", calls)
	}

	for _, response := range []struct {
		version *eosclient.MGMLeaderVersion
		err     error
	}{
		{nil, nil},
		{nil, errors.New("local MGM unavailable")},
	} {
		o.fetch = func(context.Context) (*eosclient.MGMLeaderVersion, error) {
			return response.version, response.err
		}
		o.refresh()
		o.lastRefresh = time.Now()
		families, err := registry.Gather()
		if err != nil || len(families) != 0 {
			t.Fatalf("non-leader metrics=%v, err=%v", families, err)
		}
	}
}

func TestMGMVersionCollectionDoesNotBlockScrape(t *testing.T) {
	o := NewMGMLeaderVersionCollector(&CollectorOpts{Cluster: "eospilot"})
	started := make(chan struct{})
	release := make(chan struct{})
	o.fetch = func(ctx context.Context) (*eosclient.MGMLeaderVersion, error) {
		close(started)
		select {
		case <-release:
		case <-ctx.Done():
		}
		return nil, nil
	}
	registry := prometheus.NewPedanticRegistry()
	registry.MustRegister(o)
	begin := time.Now()
	if _, err := registry.Gather(); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(begin); elapsed > 500*time.Millisecond {
		t.Fatalf("scrape waited %s for version refresh", elapsed)
	}
	<-started
	close(release)
}

func TestMGMRepeatedFailuresLogOnce(t *testing.T) {
	var logs bytes.Buffer
	previous := log.Writer()
	log.SetOutput(&logs)
	defer log.SetOutput(previous)

	o := NewMGMLeaderVersionCollector(&CollectorOpts{Cluster: "test"})
	o.fetch = func(context.Context) (*eosclient.MGMLeaderVersion, error) {
		return nil, errors.New("local MGM unavailable")
	}
	o.refresh()
	o.refresh()
	if count := strings.Count(logs.String(), "failed collecting leader MGM version"); count != 1 {
		t.Fatalf("logged repeated failure %d times: %s", count, logs.String())
	}

	o.fetch = func(context.Context) (*eosclient.MGMLeaderVersion, error) { return nil, nil }
	o.refresh()
	o.fetch = func(context.Context) (*eosclient.MGMLeaderVersion, error) {
		return nil, errors.New("local MGM unavailable again")
	}
	o.refresh()
	if count := strings.Count(logs.String(), "failed collecting leader MGM version"); count != 2 {
		t.Fatalf("did not log new failure period: %s", logs.String())
	}
}
