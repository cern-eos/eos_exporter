package eosclient

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

const counterJSON = `[{"type":"system","estimators_loop_median_us":12,"counters":{"version":1,"limit_entries":50000,"entries":[{"node_id":"/eos/node:1095/fst","app":"a\"b","uid":100,"gid":200,"bytes_read_total":18446744073709551615,"bytes_written_total":20,"read_ops_total":3,"write_ops_total":4}],"rejected_entries_total":0}}]`

func TestParseShapingCountersPreservesUnsignedIntegers(t *testing.T) {
	snapshot, err := parseIOShapingCounters(counterJSON)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.Entries[0].BytesReadTotal != ^uint64(0) {
		t.Fatal("integer precision lost")
	}
	if snapshot.Entries[0].App != "a\"b" || snapshot.System.EstimatorsLoopMedianUs.String() != "12" {
		t.Fatal(snapshot)
	}
}

func TestParseShapingCountersRejectsUnsupportedAndMalformedResponses(t *testing.T) {
	for _, raw := range []string{"", `[]`, `[{"type":"app","read_rate_bps":100}]`, `[{"type":"system","counters":{"version":2}}]`, `[{"type":"system","counters":{"version":1,"limit_entries":50000}}]`, `[{"type":"system","counters":{"version":1,"limit_entries":50000,"entries":[{"bytes_read_total":-1}]}}]`, `[{"type":"system","counters":{"version":1,"limit_entries":50000,"entries":[{},{}]}}]`} {
		if _, err := parseIOShapingCounters(raw); err == nil {
			t.Fatalf("accepted %s", raw)
		}
	}
}

func TestListShapingCountersUsesLegacyCommandAndConfiguredBinary(t *testing.T) {
	dir := t.TempDir()
	binary := filepath.Join(dir, "eos")
	// A fake client rejects any new CLI flag, so this exercises the real execution
	// path used with the unmodified 5.4 console client.
	script := "#!/bin/sh\n[ \"$*\" = 'io shaping ls --apps --json --sys' ] || exit 9\ncat <<'JSON'\n" + counterJSON + "\nJSON\n"
	if err := os.WriteFile(binary, []byte(script), 0700); err != nil {
		t.Fatal(err)
	}
	client, err := New(&Options{EosBinary: binary, Timeout: 2})
	if err != nil {
		t.Fatal(err)
	}
	snapshot, err := client.ListIOShapingCounters(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(snapshot.Entries) != 1 {
		t.Fatal(snapshot)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err = client.ListIOShapingCounters(ctx); err == nil {
		t.Fatal("ignored cancelled request")
	}
}
