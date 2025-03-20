package collector

import (
	"fmt"
	"log"
	"os"
	"regexp"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

type ErrorLogMetric struct {
	Level  string
	Source string
	Count  float64
	Host   string
}

type ErrorLogCollector struct {
	*CollectorOpts
	Count *prometheus.GaugeVec
}

// NewErrorLogCollector creates an ErrorLogCollector and instantiates
// the individual metrics that show information about the EOS error logs.
func NewErrorLogCollector(opts *CollectorOpts) *ErrorLogCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"

	return &ErrorLogCollector{
		CollectorOpts: opts,
		Count: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "error_log_stat",
				Help:        "eos error log statistics",
				ConstLabels: labels,
			},
			[]string{"level", "source", "host"},
		),
	}
}

func (o *ErrorLogCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Count,
	}
}

// scrapeAllErrorLogs gathers information about error logs from the eos error log file.
func (o *ErrorLogCollector) scrapeAllErrorLogs() ([]*ErrorLogMetric, error) {
	filePath := "/var/log/eos/mgm/error.log"
	raw, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("error reading the file %s: %v", filePath, err)
	}

	hostname, err := os.Hostname()
	if err != nil {
		return nil, fmt.Errorf("error obtaining hostname: %v", err)
	}

	errorLogMetrics := []*ErrorLogMetric{}
	rawLines := strings.Split(string(raw), "\n")

	re := regexp.MustCompile(`.*level=([^\s]+).*source=([^\s:]+)`)

	counts := make(map[struct{ Level, Source string }]int)
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		matches := re.FindStringSubmatch(rl)
		if len(matches) >= 3 {
			entry := struct{ Level, Source string }{Level: matches[1], Source: matches[2]}
			counts[entry]++
		}
	}

	maxCountWidth := 0
	for _, count := range counts {
		countStr := fmt.Sprintf("%d", count)
		if len(countStr) > maxCountWidth {
			maxCountWidth = len(countStr)
		}
	}

	for entry, count := range counts {
		countStr := fmt.Sprintf("%*d", maxCountWidth, count)
		errorLogMetrics = append(errorLogMetrics, &ErrorLogMetric{
			Level:  entry.Level,
			Source: entry.Source,
			Count:  float64(count),
			Host:   hostname,
		})

		// Printing the result, useful for debugging
		fmt.Printf("%s level=%s source=%s host=%s\n", countStr, entry.Level, entry.Source, hostname)
	}

	return errorLogMetrics, nil
}

// Collect sends all the collected error log metrics to the provided Prometheus channel.
func (o *ErrorLogCollector) Collect(ch chan<- prometheus.Metric) {
	// Collect error logs data
	errorLogMetrics, err := o.scrapeAllErrorLogs()
	if err != nil {
		log.Println("failed collecting error log metrics:", err)
		return
	}

	// Reset the metrics
	o.Count.Reset()

	// Update the metrics with the error log data
	for _, metric := range errorLogMetrics {
		o.Count.WithLabelValues(metric.Level, metric.Source, metric.Host).Set(metric.Count)
	}

	// Collect the metrics
	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// Describe sends the descriptors of each ErrorLogCollector related metrics.
func (o *ErrorLogCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}
