package collector

import (
	"testing"

	"github.com/cern-eos/eos_exporter/eosclient"
)

func TestProjectIOShapingAllFSDetail(t *testing.T) {
	stats := []*eosclient.IOShapingAllStat{
		{
			Type:         "all",
			NodeID:       "node-a:1095",
			FSID:         "10",
			App:          "xrootd",
			UID:          "1000",
			GID:          "2000",
			WindowSec:    "60",
			ReadRateBps:  "1.5",
			WriteRateBps: "2.5",
			ReadIops:     "3",
			WriteIops:    "4",
		},
		{
			Type:         "all",
			NodeID:       "node-a:1095",
			FSID:         "10",
			App:          "xrootd",
			UID:          "1000",
			GID:          "2000",
			WindowSec:    "60",
			ReadRateBps:  "10",
			WriteRateBps: "20",
			ReadIops:     "30",
			WriteIops:    "40",
		},
		{
			Type:                       "system",
			ID:                         "engine_meta",
			EstimatorsLoopMedianUs:     "1",
			ReportsProcessedPerSecMean: "2",
			SystemStatsWindowSeconds:   "15",
		},
	}

	standard, fs, entries := projectIOShapingAll(stats)
	if entries != 2 {
		t.Fatalf("expected 2 all entries, got %d", entries)
	}

	app := standard[shapingStandardKey{Type: "app", ID: "xrootd", WindowSec: "60"}]
	assertRateValues(t, app, shapingRateValues{ReadRateBps: 11.5, WriteRateBps: 22.5, ReadIops: 33, WriteIops: 44})

	uid := standard[shapingStandardKey{Type: "uid", ID: "1000", WindowSec: "60"}]
	assertRateValues(t, uid, shapingRateValues{ReadRateBps: 11.5, WriteRateBps: 22.5, ReadIops: 33, WriteIops: 44})

	gid := standard[shapingStandardKey{Type: "gid", ID: "2000", WindowSec: "60"}]
	assertRateValues(t, gid, shapingRateValues{ReadRateBps: 11.5, WriteRateBps: 22.5, ReadIops: 33, WriteIops: 44})

	node := standard[shapingStandardKey{Type: "node", ID: "node-a:1095", WindowSec: "60"}]
	assertRateValues(t, node, shapingRateValues{ReadRateBps: 11.5, WriteRateBps: 22.5, ReadIops: 33, WriteIops: 44})

	filesystem := fs[shapingFSKey{NodeID: "node-a:1095", FSID: "10", WindowSec: "60"}]
	assertRateValues(t, filesystem, shapingRateValues{ReadRateBps: 11.5, WriteRateBps: 22.5, ReadIops: 33, WriteIops: 44})
}

func TestProjectIOShapingAllAggregateDetail(t *testing.T) {
	stats := []*eosclient.IOShapingAllStat{
		{
			Type:         "all",
			NodeID:       "<unknown>",
			FSID:         "0",
			App:          "eos/balance",
			UID:          "1",
			GID:          "1",
			WindowSec:    "60",
			ReadRateBps:  "961194.67",
			WriteRateBps: "629145.60",
			ReadIops:     "0.92",
			WriteIops:    "0.60",
		},
	}

	standard, fs, entries := projectIOShapingAll(stats)
	if entries != 1 {
		t.Fatalf("expected 1 all entry, got %d", entries)
	}

	app := standard[shapingStandardKey{Type: "app", ID: "eos/balance", WindowSec: "60"}]
	assertRateValues(t, app, shapingRateValues{ReadRateBps: 961194.67, WriteRateBps: 629145.60, ReadIops: 0.92, WriteIops: 0.60})

	node := standard[shapingStandardKey{Type: "node", ID: "<unknown>", WindowSec: "60"}]
	assertRateValues(t, node, shapingRateValues{ReadRateBps: 961194.67, WriteRateBps: 629145.60, ReadIops: 0.92, WriteIops: 0.60})

	filesystem := fs[shapingFSKey{NodeID: "<unknown>", FSID: "0", WindowSec: "60"}]
	assertRateValues(t, filesystem, shapingRateValues{ReadRateBps: 961194.67, WriteRateBps: 629145.60, ReadIops: 0.92, WriteIops: 0.60})
}

func assertRateValues(t *testing.T, got, want shapingRateValues) {
	t.Helper()

	if got != want {
		t.Fatalf("got %+v, want %+v", got, want)
	}
}
