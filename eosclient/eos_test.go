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
