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

func TestParseIOShapingDisks(t *testing.T) {
	raw := `[{
		"type": "disk",
		"node_id": "st-120-100gb-fuvisz.cern.ch:1095",
		"fsid": 14658,
		"window_sec": 60,
		"read_rate_bps": 0.00,
		"write_rate_bps": 0.07,
		"read_iops": 0.00,
		"write_iops": 0.03
	}]`

	client := &Client{}
	stats, err := client.parseIOShapingDisks(raw)
	if err != nil {
		t.Fatalf("parseIOShapingDisks returned error: %v", err)
	}
	if len(stats) != 1 {
		t.Fatalf("expected one disk stat, got %d", len(stats))
	}

	stat := stats[0]
	if stat.Type != "disk" {
		t.Fatalf("expected type disk, got %q", stat.Type)
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
