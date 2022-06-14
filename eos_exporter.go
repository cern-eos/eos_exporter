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
func NewEOSExporter(instance string) *EOSExporter {
	return &EOSExporter{
		collectors: []prometheus.Collector{
			collector.NewSpaceCollector(instance),      // eos space stats
			collector.NewGroupCollector(instance),      // eos scheduling group stats
			collector.NewNodeCollector(instance),       // eos node stats
			collector.NewFSCollector(instance),         // eos filesystem stats
			collector.NewVSCollector(instance),         // eos FST versions information
			collector.NewNSCollector(instance),         // eos namespace information
			collector.NewNSActivityCollector(instance), // eos namespace activity information
			collector.NewNSBatchCollector(instance),    // eos namespace potential batch overload information
			collector.NewIOInfoCollector(instance),     // eos io stat information
			collector.NewIOAppInfoCollector(instance),  // eos io stat information per App
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
	/*c.mu.Lock()
	defer c.mu.Unlock()*/

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
}

var cmdOptions *Options = &Options{}

func init() {
	flag.StringVar(&cmdOptions.ListenAddress, "listen-address", ":9986", "Address on which to expose metrics and web interface.")
	flag.StringVar(&cmdOptions.MetricsPath, "telemetry-path", "/metrics", "Path under which to expose metrics.")
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

	fmt.Printf("Starting eos exporter for instance: %s at ", cmdOptions.EOSInstance)
	prometheus.Register(NewEOSExporter(cmdOptions.EOSInstance))
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
