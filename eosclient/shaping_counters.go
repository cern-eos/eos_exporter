package eosclient

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
)

// IOShapingCounters is the versioned monitoring snapshot from EOS 5.4.
// Totals belong to the MGM and may reset when its monitoring state restarts.
type IOShapingCounters struct {
	Version              int                `json:"version"`
	Entries              []IOShapingCounter `json:"entries"`
	RejectedEntriesTotal uint64             `json:"rejected_entries_total"`
	LimitEntries         uint64             `json:"limit_entries"`
	System               ShapingStatsJSON   `json:"-"`
}

type IOShapingCounter struct {
	NodeID            string `json:"node_id"`
	App               string `json:"app"`
	UID               uint32 `json:"uid"`
	GID               uint32 `json:"gid"`
	BytesReadTotal    uint64 `json:"bytes_read_total"`
	BytesWrittenTotal uint64 `json:"bytes_written_total"`
	ReadOpsTotal      uint64 `json:"read_ops_total"`
	WriteOpsTotal     uint64 `json:"write_ops_total"`
}

// Require complete source counters: absent fields must not look like real zeroes.
func (entry *IOShapingCounter) UnmarshalJSON(data []byte) error {
	type plain IOShapingCounter
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(data, &fields); err != nil {
		return err
	}
	for _, name := range []string{"node_id", "app", "uid", "gid", "bytes_read_total", "bytes_written_total", "read_ops_total", "write_ops_total"} {
		value, ok := fields[name]
		if !ok || string(value) == "null" {
			return fmt.Errorf("missing shaping counter field %s", name)
		}
	}
	return json.Unmarshal(data, (*plain)(entry))
}

func (c *Client) ListIOShapingCounters(ctx context.Context) (*IOShapingCounters, error) {
	ctx, cancel := c.getTimeout(ctx)
	defer cancel()
	// --sys predates --all and filesystem detail, so no new console protobuf is needed.
	cmd := exec.CommandContext(ctx, c.opt.EosBinary, "io", "shaping", "ls", "--apps", "--json", "--sys")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, fmt.Errorf("fetch shaping counters: %w", err)
	}
	return parseIOShapingCounters(stdout)
}

func parseIOShapingCounters(raw string) (*IOShapingCounters, error) {
	var rows []struct {
		ShapingStatsJSON
		Counters *IOShapingCounters `json:"counters"`
	}
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil, fmt.Errorf("decode shaping counters: %w", err)
	}
	for _, row := range rows {
		if row.Type != "system" || row.Counters == nil {
			continue
		}
		snapshot := row.Counters
		if snapshot.Version != 1 {
			return nil, fmt.Errorf("unsupported shaping counter version %d", snapshot.Version)
		}
		if snapshot.Entries == nil || snapshot.LimitEntries == 0 || len(snapshot.Entries) > 50000 {
			return nil, fmt.Errorf("invalid or oversized shaping counter snapshot")
		}
		// A duplicate source identity would double every projected counter.
		type identity struct {
			node, app string
			uid, gid  uint32
		}
		seen := make(map[identity]bool, len(snapshot.Entries))
		for _, entry := range snapshot.Entries {
			key := identity{entry.NodeID, entry.App, entry.UID, entry.GID}
			if seen[key] {
				return nil, fmt.Errorf("duplicate shaping counter identity")
			}
			seen[key] = true
		}
		snapshot.System = row.ShapingStatsJSON
		return snapshot, nil
	}
	return nil, fmt.Errorf("EOS does not expose cumulative shaping counters; install the 5.4 monitoring patch (response length %d)", len(strings.TrimSpace(raw)))
}
