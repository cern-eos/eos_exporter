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
	"sync"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/collector"
	"gopkg.in/alecthomas/kingpin.v2"
	"github.com/prometheus/common/version"
	"github.com/prometheus/common/log"
	"net/http"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// EOSExporter wraps all the EOS collectors and provides a single global exporter to extracts metrics out of.
type EOSExporter struct {
	mu         sync.Mutex
	collectors []prometheus.Collector
}

// Verify that the exporter implements the interface correctly.
var _ prometheus.Collector = &EOSExporter{}

// NewEOSExporter creates an instance to EOSExporter
func NewEOSExporter(instance string) *EOSExporter {
	return &EOSExporter{
		collectors: []prometheus.Collector{
			collector.NewSpaceCollector(instance), // eos space stats
			collector.NewGroupCollector(instance), // eos scheduling group stats
			collector.NewNodeCollector(instance), // eos node stats
			collector.NewFSCollector(instance), // eos filesystem stats
			collector.NewVSCollector(instance), // eos FST versions information
			collector.NewNSCollector(instance), // eos namespace information
			collector.NewNSActivityCollector(instance), // eos namespace activity information
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
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, cc := range c.collectors {
		cc.Collect(ch)
	}
}

func main() {
	var (
		listenAddress = kingpin.Flag("web.listen-address", "Address on which to expose metrics and web interface.").Default(":9373").String()
		metricsPath   = kingpin.Flag("web.telemetry-path", "Path under which to expose metrics.").Default("/metrics").String()
		eosInstance	  = kingpin.Arg("eos-instance","EOS instance name").Required().String()
	)

	log.AddFlags(kingpin.CommandLine)
	kingpin.Version(version.Print("eos_exporter"))
	kingpin.HelpFlag.Short('h')
	kingpin.Parse()

	log.Infoln("Starting eos_exporter", version.Info())
	log.Infoln("Build context", version.BuildContext())

	log.Infoln("Starting eos exporter for instance: %s", *eosInstance)
	prometheus.Register(NewEOSExporter(*eosInstance))

	http.Handle(*metricsPath, promhttp.Handler())
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`<html>
			<head><title>EOS Exporter</title></head>
			<body>
			<h1>EOS Exporter</h1>
			<p><a href="` + *metricsPath + `">Metrics</a></p>
			</body>
			</html>`))
	})

	log.Infoln("Listening on", *listenAddress)
	err := http.ListenAndServe(*listenAddress, nil)
	if err != nil {
		log.Fatal(err)
	}
}
