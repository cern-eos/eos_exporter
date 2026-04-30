package eosclient

import "testing"

func TestParseIOShapingConfig(t *testing.T) {
	raw := `{
		"enabled": true,
		"estimators_update_period_ms": 100,
		"fst_io_policy_update_period_ms": 100,
		"fst_io_stats_reporting_period_ms": 100,
		"detail_level": "filesystem",
		"system_stats_time_window_seconds": 15
	}`

	client := &Client{}
	config, err := client.parseIOShapingConfig(raw)
	if err != nil {
		t.Fatalf("parseIOShapingConfig returned error: %v", err)
	}

	if !config.Enabled {
		t.Fatal("expected enabled to be true")
	}
	if config.EstimatorsUpdatePeriodMs != "100" {
		t.Fatalf("expected estimators period 100, got %q", config.EstimatorsUpdatePeriodMs)
	}
	if config.FstIOPolicyUpdatePeriodMs != "100" {
		t.Fatalf("expected FST IO policy period 100, got %q", config.FstIOPolicyUpdatePeriodMs)
	}
	if config.FstIOStatsReportingPeriodMs != "100" {
		t.Fatalf("expected FST IO stats period 100, got %q", config.FstIOStatsReportingPeriodMs)
	}
	if !config.DetailFilesystem {
		t.Fatal("expected detail filesystem to be true")
	}
	if config.SystemStatsTimeWindowSeconds != "15" {
		t.Fatalf("expected system stats time window 15, got %q", config.SystemStatsTimeWindowSeconds)
	}
}

func TestParseIOShapingConfigFSDetail(t *testing.T) {
	raw := `{
		"enabled": true,
		"detail_level": "fs"
	}`

	client := &Client{}
	config, err := client.parseIOShapingConfig(raw)
	if err != nil {
		t.Fatalf("parseIOShapingConfig returned error: %v", err)
	}

	if !config.DetailFilesystem {
		t.Fatal("expected detail filesystem to be true for fs detail")
	}
}

func TestParseIOShapingConfigAggregateDetail(t *testing.T) {
	raw := `{
		"enabled": false,
		"detail_level": "aggregate"
	}`

	client := &Client{}
	config, err := client.parseIOShapingConfig(raw)
	if err != nil {
		t.Fatalf("parseIOShapingConfig returned error: %v", err)
	}

	if config.Enabled {
		t.Fatal("expected enabled to be false")
	}
	if config.DetailFilesystem {
		t.Fatal("expected detail filesystem to be false for aggregate detail")
	}
}

func TestParseIOShapingFS(t *testing.T) {
	raw := `[{
		"type": "fs",
		"node_id": "st-120-100gb-fuvisz.cern.ch:1095",
		"fsid": 14658,
		"window_sec": 60,
		"read_rate_bps": 0.00,
		"write_rate_bps": 0.07,
		"read_iops": 0.00,
		"write_iops": 0.03
	}]`

	client := &Client{}
	stats, err := client.parseIOShapingFS(raw)
	if err != nil {
		t.Fatalf("parseIOShapingFS returned error: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("expected one filesystem stat, got %d", len(stats))
	}

	stat := stats[0]
	if stat.Type != "fs" {
		t.Fatalf("expected type fs, got %q", stat.Type)
	}
	if stat.NodeID != "st-120-100gb-fuvisz.cern.ch:1095" {
		t.Fatalf("expected node id st-120-100gb-fuvisz.cern.ch:1095, got %q", stat.NodeID)
	}
	if stat.FSID != "14658" {
		t.Fatalf("expected fsid 14658, got %q", stat.FSID)
	}
	if stat.WindowSec != "60" {
		t.Fatalf("expected window 60, got %q", stat.WindowSec)
	}
	if stat.ReadRateBps != "0.00" {
		t.Fatalf("expected read rate 0.00, got %q", stat.ReadRateBps)
	}
	if stat.WriteRateBps != "0.07" {
		t.Fatalf("expected write rate 0.07, got %q", stat.WriteRateBps)
	}
	if stat.ReadIops != "0.00" {
		t.Fatalf("expected read iops 0.00, got %q", stat.ReadIops)
	}
	if stat.WriteIops != "0.03" {
		t.Fatalf("expected write iops 0.03, got %q", stat.WriteIops)
	}
}

func TestParseIOShapingAll(t *testing.T) {
	raw := `[{
		"type": "all",
		"node_id": "st-120-100gb-fuvisz.cern.ch:1095",
		"fsid": 1234,
		"app": "eos/fusex",
		"uid": 0,
		"gid": 0,
		"window_sec": 5,
		"read_rate_bps": 0,
		"write_rate_bps": 10,
		"read_iops": 0,
		"write_iops": 2
	}]`

	client := &Client{}
	stats, err := client.parseIOShapingAll(raw)
	if err != nil {
		t.Fatalf("parseIOShapingAll returned error: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("expected one all-tags stat, got %d", len(stats))
	}

	stat := stats[0]
	if stat.Type != "all" {
		t.Fatalf("expected type all, got %q", stat.Type)
	}
	if stat.NodeID != "st-120-100gb-fuvisz.cern.ch:1095" {
		t.Fatalf("expected node id st-120-100gb-fuvisz.cern.ch:1095, got %q", stat.NodeID)
	}
	if stat.FSID != "1234" {
		t.Fatalf("expected fsid 1234, got %q", stat.FSID)
	}
	if stat.App != "eos/fusex" {
		t.Fatalf("expected app eos/fusex, got %q", stat.App)
	}
	if stat.UID != "0" {
		t.Fatalf("expected uid 0, got %q", stat.UID)
	}
	if stat.GID != "0" {
		t.Fatalf("expected gid 0, got %q", stat.GID)
	}
	if stat.WindowSec != "5" {
		t.Fatalf("expected window 5, got %q", stat.WindowSec)
	}
	if stat.ReadRateBps != "0" {
		t.Fatalf("expected read rate 0, got %q", stat.ReadRateBps)
	}
	if stat.WriteRateBps != "10" {
		t.Fatalf("expected write rate 10, got %q", stat.WriteRateBps)
	}
	if stat.ReadIops != "0" {
		t.Fatalf("expected read iops 0, got %q", stat.ReadIops)
	}
	if stat.WriteIops != "2" {
		t.Fatalf("expected write iops 2, got %q", stat.WriteIops)
	}
}
