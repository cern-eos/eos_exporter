package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
	//"os"
	//"bufio"
	//"fmt"
	//"strings"
)

type NSCollector struct {
	Value		*prometheus.GaugeVec
	Sum		*prometheus.GaugeVec
	Last_5s		*prometheus.GaugeVec
	Last_60s	*prometheus.GaugeVec
	Last_300s	*prometheus.GaugeVec
	Last_3600s	*prometheus.GaugeVec
}

//NewNSCollector creates an instance of the NSCollector and instantiates
// the individual metrics that show information about the NS.
func NewNSCollector(cluster string) *NSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &NSCollector{
		Value: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_parameter_total",
				Help:        "Value: Is the value of the current parameter as metric.",
				ConstLabels: labels,
			},
			[]string{"parameter"},
		),
		Sum: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_sum_total",
				Help:        "Sum: Cummulated ocurrences of the operation.",
				ConstLabels: labels,
			},
			[]string{"user", "operation"},
		),
		Last_5s: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_last5s_total",
				Help:        "Last_5s: Cummulated ocurrences of the operation in the last 5s.",
				ConstLabels: labels,
			},
			[]string{"user", "operation"},
		),
		Last_60s: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_last1min_total",
				Help:        "Last_60s: Cummulated ocurrences of the operation in the last minute.",
				ConstLabels: labels,
			},
			[]string{"user", "operation"},
		),
		Last_300s: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_last5min_total",
				Help:        "Last_300s: Cummulated ocurrences of the operation in the last 5 min.",
				ConstLabels: labels,
			},
			[]string{"user", "operation"},
		),
		Last_3600s: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_last1h_total",
				Help:        "Last_3600s: Cummulated ocurrences of the operation in the last hour.",
				ConstLabels: labels,
			},
			[]string{"user", "operation"},
		),
	}
}

func (o *NSCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Value,
		o.Sum,
		o.Last_5s,
		o.Last_60s,
		o.Last_300s,
		o.Last_3600s,
	}
}

func (o *NSCollector) collectNSDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, mdsact, err := client.ListNS(context.Background())
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		var value float64
		if m.Parameter == "ns.boot.status" {
			switch stat := m.Value; stat {
				case "booted":
					value = 1
				default:
					value = 0
			}
				o.Value.WithLabelValues(m.Parameter).Set(float64(value))
		} else {

			value, err := strconv.ParseFloat(m.Value, 64)
			if err == nil {
				o.Value.WithLabelValues(m.Parameter).Set(value)
			}
		}
	}

	for _, n := range mdsact{

		// Sum

		sum, err := strconv.ParseFloat(n.Sum, 64)
		if err == nil {
			o.Sum.WithLabelValues(n.User, n.Operation).Set(sum)
		}

		// Last_5s

		last_5s, err := strconv.ParseFloat(n.Last_5s, 64)
		if err == nil {
			o.Last_5s.WithLabelValues(n.User, n.Operation).Set(last_5s)
		}

		// Last_60s

		last_1min, err := strconv.ParseFloat(n.Last_60s, 64)
		if err == nil {
			o.Last_60s.WithLabelValues(n.User, n.Operation).Set(last_1min)
		}

		// Last_300s

		last_5min, err := strconv.ParseFloat(n.Last_300s, 64)
		if err == nil {
			o.Last_300s.WithLabelValues(n.User, n.Operation).Set(last_5min)
		}

		// Last_3600s

		last_1h, err := strconv.ParseFloat(n.Last_3600s, 64)
		if err == nil {
			o.Last_3600s.WithLabelValues(n.User, n.Operation).Set(last_1h)
		}

	}

	return nil

} // collectNSDF()

// Describe sends the descriptors of each NSCollector related metrics we have defined
func (o *NSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *NSCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectNSDF(); err != nil {
		log.Println("failed collecting space metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
