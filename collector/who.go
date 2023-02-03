package collector

import (
	"context"
	"fmt"
	"os"
	// "time"
	"log"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

// eos who -a -m provides 3 clusters of information
// a) Aggregation of number of sessions by protocol, examples:
// auth=gsi nsessions=2
// auth=https nsessions=3093
// b) Aggregation by uid
// uid=982 nsessions=2
// uid=983 nsessions=2
// c) Client info
// client=yyy@xxx.cern.ch uid=yyy auth=https idle=66 gateway="xxx.cern.ch" app=http
// Because a) and b) can be derived from c), we only report c) in the metric
// Aggregation on fields from c) can be done in the monitoring system
// This collector provides metrics based on c)

type WhoCollector struct {
	SessionNumber *prometheus.GaugeVec
	file          *os.File
}

//NewWhoCollector creates an cluster of the WhoCollector
func NewWhoCollector(cluster string) *WhoCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	namespace := "eos"

	//f, err := os.OpenFile("/var/tmp/eos_exporter_debug.txt", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	//if err != nil {
	//	panic(err)
	//}

	return &WhoCollector{
		//file: f,
		SessionNumber: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "who",
				Help:        "sessions opened",
				ConstLabels: labels,
			},
			[]string{"uid", "auth", "gateway", "app"},
		),
	}
}

func (o *WhoCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.SessionNumber,
	}
}

func (o *WhoCollector) collectWhoDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	whos, err := client.Who(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	//t := time.Now().String()
	//o.file.WriteString(fmt.Sprintf("TIME (%s)\n", t))

	// aggregate by value
	counter := map[string]int{}
	for _, m := range whos {
		if _, ok := counter[m.Serialized]; ok {
			counter[m.Serialized]++
		} else {
			counter[m.Serialized] = 1
		}
	}

	// a GaugeVector keeps its state for any combination of labels until the process is restarted.
	// For metrics obtained from systems like EOS that do not produce a complete set of metrics (only the active metrics)
	// then we risk to expose these metrics forever until the next process restart.
	// To workaround this, we reset the gauge vectors when collecting metrics.

	o.SessionNumber.Reset()
	for i, v := range counter {
		tokens := strings.Split(i, ":::")
		uid, auth, gateway, app := tokens[0], tokens[1], tokens[2], tokens[3]
		o.SessionNumber.WithLabelValues(uid, auth, gateway, app).Set(float64(v))
		//o.file.WriteString(fmt.Sprintf("%s setting (%s)=%d\n", t, i, v))
	}

	return nil

} // collectWhoDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *WhoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		fmt.Print(metric)
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *WhoCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectWhoDF(); err != nil {
		log.Println("failed collecting who  metrics:", err)
	}

	for _, collector := range o.collectorList() {
		collector.Collect(ch)
	}
}
