//   Copyright 2015 The Prometheus Authors
//   Licensed under the Apache License, Version 2.0 (the "License");
//   you may not use this file except in compliance with the License.
//   You may obtain a copy of the License at
//
//       http://www.apache.org/licenses/LICENSE-2.0
//
//   Unless required by applicable law or agreed to in writing, software
//   distributed under the License is distributed on an "AS IS" BASIS,
//   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//   See the License for the specific language governing permissions and
//   limitations under the License.

// Command EOS_exporter provides a Prometheus exporter for a EOS instance.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors" // <-- New Import
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/cern-eos/eos_exporter/collector"

	_ "embed"
)

//go:generate sh get_build_info.sh
var (
	//go:embed .git_commit
	gitCommit string
	//go:embed .build_date
	buildDate string
	//go:embed .version
	version string
	//go:embed .go_version
	goVersion string
)

// List of all available collectors and their constructor functions wrapped to return the interface
var availableCollectors = []struct {
	name    string
	creator func(*collector.CollectorOpts) prometheus.Collector
}{
	{"space", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewSpaceCollector(opts) }},
	{"group", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewGroupCollector(opts) }},
	{"node", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewNodeCollector(opts) }},
	{"fs", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewFSCollector(opts) }},
	{"io_info", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewIOInfoCollector(opts) }},
	{"io_app_info", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewIOAppInfoCollector(opts) }},
	{"traffic_shaping_io", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewIOShapingCollector(opts) }},
	{"traffic_shaping_policy", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewIOShapingPolicyCollector(opts)
	}},
	{"ns", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewNSCollector(opts) }},
	{"ns_activity", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewNSActivityCollector(opts)
	}},
	{"ns_batch", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewNSBatchCollector(opts) }},
	{"recycle", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewRecycleCollector(opts) }},
	{"who", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewWhoCollector(opts) }},
	{"quotas", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewQuotasCollector(opts) }},
	{"fsck", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewFsckCollector(opts) }},
	{"fusex", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewFusexCollector(opts) }},
	{"inspector_layout", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorLayoutCollector(opts)
	}},
	{"inspector_accesstime_volume", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorAccessTimeVolumeCollector(opts)
	}},
	{"inspector_accesstime_files", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorAccessTimeFilesCollector(opts)
	}},
	{"inspector_birthtime_volume", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorBirthTimeVolumeCollector(opts)
	}},
	{"inspector_birthtime_files", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorBirthTimeFilesCollector(opts)
	}},
	{"inspector_groupcost_disk", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorGroupCostDiskCollector(opts)
	}},
	{"inspector_groupcost_disktbyears", func(opts *collector.CollectorOpts) prometheus.Collector {
		return collector.NewInspectorGroupCostDiskTBYearsCollector(opts)
	}},
	{"audit", func(opts *collector.CollectorOpts) prometheus.Collector { return collector.NewAuditCollector(opts) }},
}

// EOSExporter wraps a list of registered EOS collectors
type EOSExporter struct {
	mu         sync.RWMutex
	collectors []prometheus.Collector
}

var _ prometheus.Collector = &EOSExporter{}

func (c *EOSExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, cc := range c.collectors {
		cc.Describe(ch)
	}
}

func (c *EOSExporter) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, cc := range c.collectors {
		cc.Collect(ch)
	}
}

type Options struct {
	ListenAddress     string
	ListenAddressFast string
	MetricsPath       string
	EOSInstance       string
	Collectors        string
	Version           bool
	Help              bool
	Timeout           int
	AuditLogPath      string
	AuditPollInterval int
}

var cmdOptions *Options = &Options{}

func init() {
	flag.StringVar(&cmdOptions.ListenAddress, "listen-address", ":9986", "Address on which to expose standard metrics.")
	flag.StringVar(&cmdOptions.ListenAddressFast, "listen-address-fast", ":9987", "Address on which to expose fast metrics.")
	flag.StringVar(&cmdOptions.MetricsPath, "telemetry-path", "/metrics", "Path under which to expose metrics.")
	flag.IntVar(&cmdOptions.Timeout, "timeout", 30, "Number of seconds to timeout when querying EOS.")
	flag.StringVar(&cmdOptions.EOSInstance, "eos-instance", "", "EOS instance name.")
	flag.StringVar(&cmdOptions.Collectors, "collectors", "all", "Comma-separated list of standard collectors to enable (e.g. 'space,node'). Default is 'all'.")
	flag.StringVar(&cmdOptions.AuditLogPath, "audit-log-path", "/var/log/eos/mgm/audit/audit.zstd", "Path to the EOS audit log symlink. Default is standard EOS path.")
	flag.IntVar(&cmdOptions.AuditPollInterval, "audit-poll-interval", 30, "Interval in seconds to check for new audit log files.")
	flag.BoolVar(&cmdOptions.Help, "help", false, "Show the help and exit.")
	flag.BoolVar(&cmdOptions.Version, "version", false, "Show the version and exit.")
	flag.Parse()

	if err := validate(); err != nil {
		fmt.Printf("Error: %s\n", err.Error())
		printUsage()
	}
}

func validate() error {
	if cmdOptions.Help || cmdOptions.Version {
		return nil
	}

	if cmdOptions.EOSInstance == "" {
		return errors.New("specify an EOS instance using the --eos-instance flag")
	}

	return nil
}

func printUsage() {
	fmt.Printf("Usage: %s [flags]\n", os.Args[0])
	flag.PrintDefaults()
	os.Exit(1)
}

func printVersion() {
	msg := `%s [%s]
  go version = %s
  build date = %s
  commit = %s
`
	fmt.Printf(msg, os.Args[0], version, goVersion, buildDate, gitCommit)
	os.Exit(0)
}

// createServer builds an HTTP server for a specific registry to isolate the metrics paths cleanly
func createServer(address, path string, registry *prometheus.Registry) *http.Server {
	mux := http.NewServeMux()
	mux.Handle(path, promhttp.HandlerFor(registry, promhttp.HandlerOpts{}))
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
          <head><title>EOS Exporter</title></head>
          <body>
          <h1>EOS Exporter</h1>
          <p><a href="` + path + `">Metrics</a></p>
          </body>
          </html>`))
	})
	return &http.Server{Addr: address, Handler: mux}
}

func main() {
	if cmdOptions.Help {
		printUsage()
	}

	if cmdOptions.Version {
		printVersion()
	}

	collectorOpts := &collector.CollectorOpts{
		Cluster:           cmdOptions.EOSInstance,
		Timeout:           cmdOptions.Timeout,
		AuditLogPath:      cmdOptions.AuditLogPath,
		AuditPollInterval: cmdOptions.AuditPollInterval,
	}

	log.Println("Starting eos exporter for instance", cmdOptions.EOSInstance)

	fastCollectorsSet := map[string]bool{
		"traffic_shaping_io":     true,
		"traffic_shaping_policy": true,
		// Add future fast metrics here
	}

	// Fast metrics will not be exposed in the standard endpoint to avoid duplication!
	isFastCollector := func(name string) bool {
		return fastCollectorsSet[name]
	}

	requestedMap := make(map[string]bool)
	if cmdOptions.Collectors != "all" && cmdOptions.Collectors != "" {
		for _, r := range strings.Split(cmdOptions.Collectors, ",") {
			requestedMap[strings.TrimSpace(r)] = true
		}
	}

	var slowCollectors []prometheus.Collector
	var fastCollectors []prometheus.Collector

	// Distribute collectors based on type and flags
	for _, c := range availableCollectors {
		if isFastCollector(c.name) {
			// Fast collectors are ALWAYS enabled and routed to the fast port
			fastCollectors = append(fastCollectors, c.creator(collectorOpts))
		} else {
			// Slow collectors obey the --collectors flag
			if cmdOptions.Collectors == "all" || cmdOptions.Collectors == "" || requestedMap[c.name] {
				slowCollectors = append(slowCollectors, c.creator(collectorOpts))
			}
		}
	}

	fastRegistry := prometheus.NewRegistry()
	if len(fastCollectors) > 0 {
		fastRegistry.MustRegister(&EOSExporter{collectors: fastCollectors})
	}
	fastServer := createServer(cmdOptions.ListenAddressFast, cmdOptions.MetricsPath, fastRegistry)

	stdRegistry := prometheus.NewRegistry()

	stdRegistry.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	stdRegistry.MustRegister(collectors.NewGoCollector())

	if len(slowCollectors) > 0 {
		stdRegistry.MustRegister(&EOSExporter{collectors: slowCollectors})
	}

	stdServer := createServer(cmdOptions.ListenAddress, cmdOptions.MetricsPath, stdRegistry)

	go func() {
		log.Println("Fast metrics listening on", cmdOptions.ListenAddressFast)
		if err := fastServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Fast server failed: %v", err)
		}
	}()

	go func() {
		log.Println("Standard metrics listening on", cmdOptions.ListenAddress)
		if err := stdServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Fatalf("Standard server failed: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)

	<-quit
	log.Println("Interrupt signal received. Shutting down servers gracefully...")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		if err := fastServer.Shutdown(ctx); err != nil {
			log.Printf("Fast server shutdown error: %v", err)
		}
	}()

	go func() {
		defer wg.Done()
		if err := stdServer.Shutdown(ctx); err != nil {
			log.Printf("Standard server shutdown error: %v", err)
		}
	}()

	wg.Wait()
	log.Println("EOS Exporter successfully stopped.")
}
