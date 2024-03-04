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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	// "github.com/prometheus/common/log"

	// "github.com/prometheus/common/log"
	/* // For enabling profile mode in Go
	"github.com/pkg/profile"*/
	"gitlab.cern.ch/rvalverd/eos_exporter/collector"

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
	// go:embed .go_version
	goVersion string
)

// EOSExporter wraps all the EOS collectors and provides a single global exporter to extracts metrics out of.
type EOSExporter struct {
	mu         sync.RWMutex
	collectors []prometheus.Collector
}

// Verify that the exporter implements the interface correctly.
var _ prometheus.Collector = &EOSExporter{}

// NewEOSExporter creates an instance to EOSExporter
func NewEOSExporter(opts *collector.CollectorOpts) *EOSExporter {
	return &EOSExporter{
		collectors: []prometheus.Collector{
			collector.NewSpaceCollector(opts),                         // eos space stats
			collector.NewGroupCollector(opts),                         // eos scheduling group stats
			collector.NewNodeCollector(opts),                          // eos node stats
			collector.NewFSCollector(opts),                            // eos filesystem stats
			collector.NewIOInfoCollector(opts),                        // eos io stat information
			collector.NewIOAppInfoCollector(opts),                     // eos io stat information per App
			collector.NewNSCollector(opts),                            // eos namespace information
			collector.NewNSActivityCollector(opts),                    // eos namespace activity information
			collector.NewNSBatchCollector(opts),                       // eos namespace potential batch overload information
			collector.NewRecycleCollector(opts),                       // eos recycle bin information
			collector.NewWhoCollector(opts),                           // eos who information
			collector.NewFsckCollector(opts),                          // eos fsck information
			collector.NewFusexCollector(opts),                         // eos fusex information
			collector.NewInspectorLayoutCollector(opts),               // eos inspector layout information
			collector.NewInspectorAccessTimeVolumeCollector(opts),     // eos inspector accesstime volume information
			collector.NewInspectorAccessTimeFilesCollector(opts),      // eos inspector accesstime files information
			collector.NewInspectorBirthTimeVolumeCollector(opts),      // eos inspector birthtime volume information
			collector.NewInspectorBirthTimeFilesCollector(opts),       // eos inspector birthtime files information
			collector.NewInspectorGroupCostDiskCollector(opts),        // eos inspector group cost disk information
			collector.NewInspectorGroupCostDiskTBYearsCollector(opts), // eos inspector group cost disk tbyears information

		},
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
	// TODO (gdelmont): check that ListenAddress is a valid address:port string

	// skip all check when either help or version flags are provided
	if cmdOptions.Help || cmdOptions.Version {
		return nil
	}

	// EOSInstamce is required
	if cmdOptions.EOSInstance == "" {
		return errors.New("Specify an EOS instance using the -eos-instance flag")
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
	if err := prometheus.Register(NewEOSExporter(collectorOpts)); err != nil {
		log.Fatal(err)
	}
	/* Enable Goroutine profiling
	//defer profile.Start(profile.GoroutineProfile).Stop()*/

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
