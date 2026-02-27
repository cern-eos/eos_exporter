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
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"

	"github.com/prometheus/client_golang/prometheus"
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
}

// EOSExporter wraps all the EOS collectors and provides a single global exporter to extracts metrics out of.
type EOSExporter struct {
	mu         sync.RWMutex
	collectors []prometheus.Collector
}

// Verify that the exporter implements the interface correctly.
var _ prometheus.Collector = &EOSExporter{}

// NewEOSExporter creates an instance to EOSExporter based on the requested collectors
func NewEOSExporter(opts *collector.CollectorOpts, enabled string) *EOSExporter {
	var activeCollectors []prometheus.Collector

	if enabled == "all" || enabled == "" {
		// Enable all collectors
		for _, c := range availableCollectors {
			activeCollectors = append(activeCollectors, c.creator(opts))
		}
	} else {
		// Parse comma-separated list
		requested := strings.Split(enabled, ",")
		requestedMap := make(map[string]bool)
		for _, r := range requested {
			requestedMap[strings.TrimSpace(r)] = true
		}

		// Enable only the requested ones
		for _, c := range availableCollectors {
			if requestedMap[c.name] {
				activeCollectors = append(activeCollectors, c.creator(opts))
				log.Printf("Enabled collector: %s", c.name)
			}
		}
	}

	return &EOSExporter{
		collectors: activeCollectors,
	}
}

// Describe sends all the descriptors of the collectors included to the provided channel.
func (c *EOSExporter) Describe(ch chan<- *prometheus.Desc) {
	for _, cc := range c.collectors {
		cc.Describe(ch)
	}
}

// Collect sends the collected metrics from each of the collectors to prometheus.
func (c *EOSExporter) Collect(ch chan<- prometheus.Metric) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for _, cc := range c.collectors {
		cc.Collect(ch)
	}
}

type Options struct {
	ListenAddress string
	MetricsPath   string
	EOSInstance   string
	Collectors    string
	Version       bool
	Help          bool
	Timeout       int
}

var cmdOptions *Options = &Options{}

func init() {
	flag.StringVar(&cmdOptions.ListenAddress, "listen-address", ":9986", "Address on which to expose metrics and web interface.")
	flag.StringVar(&cmdOptions.MetricsPath, "telemetry-path", "/metrics", "Path under which to expose metrics.")
	flag.IntVar(&cmdOptions.Timeout, "timeout", 30, "Number of seconds to timeout when querying EOS.")
	flag.StringVar(&cmdOptions.EOSInstance, "eos-instance", "", "EOS instance name.")
	flag.StringVar(&cmdOptions.Collectors, "collectors", "all", "Comma-separated list of collectors to enable (e.g. 'space,node,traffic_shaping_io,traffic_shaping_policy'). Default is 'all'.")
	flag.BoolVar(&cmdOptions.Help, "help", false, "Show the help and exit.")
	flag.BoolVar(&cmdOptions.Version, "version", false, "Show the version and exit.")
	flag.Parse()

	err := validate()
	if err != nil {
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

func main() {

	if cmdOptions.Help {
		printUsage()
	}

	if cmdOptions.Version {
		printVersion()
	}

	collectorOpts := &collector.CollectorOpts{
		Cluster: cmdOptions.EOSInstance,
		Timeout: cmdOptions.Timeout,
	}

	log.Println("Starting eos exporter for instance", cmdOptions.EOSInstance)

	exporter := NewEOSExporter(collectorOpts, cmdOptions.Collectors)
	if err := prometheus.Register(exporter); err != nil {
		log.Fatal(err)
	}

	http.Handle(cmdOptions.MetricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>EOS Exporter</title></head>
			<body>
			<h1>EOS Exporter</h1>
			<p><a href="` + cmdOptions.MetricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Println("Listening on", cmdOptions.ListenAddress)
	err := http.ListenAndServe(cmdOptions.ListenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
