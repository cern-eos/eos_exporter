package collector

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/klauspost/compress/zstd"
	"github.com/prometheus/client_golang/prometheus"
)

// AuditEvent represents a single EOS audit log event
type AuditEvent struct {
	Timestamp string `json:"timestamp"`
	Operation string `json:"operation"`
	ClientIP  string `json:"client_ip"`
	Account   string `json:"account"`
	UUID      string `json:"uuid"`
	Auth      struct {
		Mechanism string `json:"mechanism"`
	} `json:"auth"`
	After struct {
		Size string `json:"size"`
	} `json:"after"`
}

// AuditState maintains state between scrapes for the audit collector
type AuditState struct {
	mu            sync.Mutex
	openFiles     map[string]int64 // UUID → CREATE timestamp
	lastProcessed string           // Last processed file path
}

func newAuditState() *AuditState {
	return &AuditState{
		openFiles: make(map[string]int64),
	}
}

// AuditCollector collects EOS audit log metrics
type AuditCollector struct {
	*CollectorOpts

	// Metrics
	OperationTotal   *prometheus.CounterVec
	WriteBytesTotal  *prometheus.CounterVec
	LifecycleSeconds *prometheus.CounterVec

	// State management
	state *AuditState

	// Background processing
	stopCh  chan struct{}
	wg      sync.WaitGroup
	started bool
	mu      sync.Mutex
}

// NewAuditCollector creates a new AuditCollector
func NewAuditCollector(opts *CollectorOpts) *AuditCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	ac := &AuditCollector{
		CollectorOpts: opts,
		OperationTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   "eos",
				Name:        "audit_operations_total",
				Help:        "Total number of EOS operations",
				ConstLabels: labels,
			},
			[]string{"operation", "auth", "account"},
		),
		WriteBytesTotal: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   "eos",
				Name:        "audit_write_bytes_total",
				Help:        "Total bytes written",
				ConstLabels: labels,
			},
			[]string{"auth", "client_ip"},
		),
		LifecycleSeconds: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Namespace:   "eos",
				Name:        "audit_lifecycle_seconds_total",
				Help:        "Total file lifecycle duration (CREATE to DELETE)",
				ConstLabels: labels,
			},
			[]string{"auth", "account"},
		),
		state:  newAuditState(),
		stopCh: make(chan struct{}),
	}

	// Always start watcher - it will log if path doesn't exist
	ac.startWatcher()

	return ac
}

// startWatcher starts the background goroutine that watches for log rotations
func (c *AuditCollector) startWatcher() {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.started {
		return
	}

	// Check if audit log path exists before starting
	if _, err := os.Stat(c.AuditLogPath); err != nil {
		log.Printf("audit: log path %s not accessible, watcher disabled: %v", c.AuditLogPath, err)
		return
	}

	c.started = true
	c.wg.Add(1)

	go func() {
		defer c.wg.Done()
		c.watchLoop()
	}()

	log.Printf("audit: started watcher for %s (poll interval: %ds)", c.AuditLogPath, c.AuditPollInterval)
}

// Stop gracefully stops the audit collector
func (c *AuditCollector) Stop() {
	c.mu.Lock()
	if !c.started {
		c.mu.Unlock()
		return
	}
	c.mu.Unlock()

	close(c.stopCh)
	c.wg.Wait()
}

// watchLoop monitors for log file rotations and processes closed files
func (c *AuditCollector) watchLoop() {
	pollInterval := 30 * time.Second
	if c.AuditPollInterval > 0 {
		pollInterval = time.Duration(c.AuditPollInterval) * time.Second
	}

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-c.stopCh:
			return
		case <-ticker.C:
			if err := c.checkAndProcessNewFile(); err != nil {
				log.Printf("audit collector: %v", err)
			}
		}
	}
}

// checkAndProcessNewFile checks if there's a new closed log file to process
func (c *AuditCollector) checkAndProcessNewFile() error {
	closed, err := c.lastClosedFile()
	if err != nil {
		return fmt.Errorf("finding closed file: %w", err)
	}

	c.state.mu.Lock()
	lastProcessed := c.state.lastProcessed
	c.state.mu.Unlock()

	if closed == lastProcessed {
		return nil
	}

	log.Printf("audit: processing %s", filepath.Base(closed))
	if err := c.parseClosedFile(closed); err != nil {
		return fmt.Errorf("parsing %s: %w", closed, err)
	}

	c.state.mu.Lock()
	c.state.lastProcessed = closed
	c.state.mu.Unlock()

	log.Printf("audit: completed %s", filepath.Base(closed))
	return nil
}

// lastClosedFile returns the most recent closed audit log file
func (c *AuditCollector) lastClosedFile() (string, error) {
	symlink := c.AuditLogPath
	dir := filepath.Dir(symlink)

	// Get the currently active file
	active, err := os.Readlink(symlink)
	if err != nil {
		return "", fmt.Errorf("readlink %s: %w", symlink, err)
	}
	active = filepath.Base(active)

	// Find all audit-*.zst files
	entries, err := filepath.Glob(filepath.Join(dir, "audit-*.zst"))
	if err != nil || len(entries) == 0 {
		return "", fmt.Errorf("no audit-*.zst files in %s", dir)
	}

	// Sort by name (YYYYMMDD-HHMMSS format sorts lexicographically)
	sort.Strings(entries)

	// Return the most recent file that is not the active one
	for i := len(entries) - 1; i >= 0; i-- {
		if filepath.Base(entries[i]) != active {
			return entries[i], nil
		}
	}

	return "", fmt.Errorf("no closed file found")
}

// parseClosedFile decompresses and processes a closed audit log file
func (c *AuditCollector) parseClosedFile(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	compressed, err := io.ReadAll(f)
	if err != nil {
		return err
	}

	dec, err := zstd.NewReader(nil)
	if err != nil {
		return err
	}
	defer dec.Close()

	decompressed, err := dec.DecodeAll(compressed, nil)
	if err != nil {
		return err
	}

	scanner := bufio.NewScanner(bytes.NewReader(decompressed))
	scanner.Buffer(make([]byte, 256*1024), 256*1024)

	for scanner.Scan() {
		var ev AuditEvent
		if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
			continue
		}
		c.processEvent(ev)
	}

	return scanner.Err()
}

// processEvent processes a single audit event and updates metrics
func (c *AuditCollector) processEvent(ev AuditEvent) {
	op := ev.Operation
	auth := ev.Auth.Mechanism
	account := ev.Account

	// Increment operation counter
	c.OperationTotal.WithLabelValues(op, auth, account).Inc()

	// Track write bytes
	if op == "WRITE" && ev.After.Size != "" {
		var sz int64
		fmt.Sscanf(ev.After.Size, "%d", &sz)
		c.WriteBytesTotal.WithLabelValues(auth, ev.ClientIP).Add(float64(sz))
	}

	// Track file lifecycle
	if ev.UUID != "" {
		c.state.mu.Lock()
		switch op {
		case "CREATE":
			var ts int64
			fmt.Sscanf(ev.Timestamp, "%d", &ts)
			c.state.openFiles[ev.UUID] = ts
		case "DELETE":
			if start, ok := c.state.openFiles[ev.UUID]; ok {
				var ts int64
				fmt.Sscanf(ev.Timestamp, "%d", &ts)
				duration := ts - start
				c.LifecycleSeconds.WithLabelValues(auth, account).Add(float64(duration))
				delete(c.state.openFiles, ev.UUID)
			}
		}
		c.state.mu.Unlock()
	}
}

func (c *AuditCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		c.OperationTotal,
		c.WriteBytesTotal,
		c.LifecycleSeconds,
	}
}

// Describe sends the descriptors of each AuditCollector metric
func (c *AuditCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range c.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel
func (c *AuditCollector) Collect(ch chan<- prometheus.Metric) {
	// The actual collection happens in the background watcher
	// This just reports the current state of the counters
	for _, metric := range c.collectorList() {
		metric.Collect(ch)
	}
}

// CollectWithContext allows for context-aware collection (optional interface)
func (c *AuditCollector) CollectWithContext(ctx context.Context, ch chan<- prometheus.Metric) {
	c.Collect(ch)
}
