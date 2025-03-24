package collector

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/prometheus/client_golang/prometheus"
)

type ErrorLogMetric struct {
	Func   string
	Level  string
	Unit   string
	Source string
	Msg    string
	Count  float64
}

type ErrorLogCollector struct {
	*CollectorOpts
	Count       *prometheus.GaugeVec
	lastOffset  int64
	logFilePath string
	mutex       sync.Mutex
}

var (
	re = regexp.MustCompile(`func=(\w+)\s+level=(\w+).*?unit=fst@([^:\s]+)(?::\d+)?\s.*?source=([^\s]+).*?msg="([^"]*)"`)
)

func NewErrorLogCollector(opts *CollectorOpts) *ErrorLogCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	collector := &ErrorLogCollector{
		CollectorOpts: opts,
		Count: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   "eos",
				Name:        "error_log_stat",
				Help:        "EOS error log statistics",
				ConstLabels: labels,
			},
			[]string{"func", "level", "source", "unit", "msg"},
		),
		logFilePath: "/var/log/eos/mgm/error.log",
	}

	go collector.startScraping()

	return collector
}

func sanitizeUTF8(input string) string {
	if utf8.ValidString(input) {
		return input
	}
	var output strings.Builder
	for _, r := range input {
		if r != utf8.RuneError {
			output.WriteRune(r)
		}
	}
	return output.String()
}

func (o *ErrorLogCollector) scrapeAllErrorLogs() ([]*ErrorLogMetric, error) {
	o.mutex.Lock()
	defer o.mutex.Unlock()

	file, err := os.Open(o.logFilePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %v", o.logFilePath, err)
	}
	defer file.Close()

	file.Seek(o.lastOffset, 0)

	scanner := bufio.NewScanner(file)
	counts := make(map[ErrorLogMetric]int)

	for scanner.Scan() {
		line := scanner.Text()
		matches := re.FindStringSubmatch(line)
		if len(matches) == 6 {
			entry := ErrorLogMetric{
				Func:   matches[1],
				Level:  matches[2],
				Unit:   matches[3],
				Source: matches[4],
				Msg:    sanitizeUTF8(matches[5]),
			}
			counts[entry]++
		}
	}

	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("error scanning file: %v", err)
	}

	newOffset, _ := file.Seek(0, os.SEEK_CUR)
	o.lastOffset = newOffset

	var metrics []*ErrorLogMetric
	for entry, count := range counts {
		entry.Count = float64(count)
		metrics = append(metrics, &entry)
	}

	return metrics, nil
}

func (o *ErrorLogCollector) startScraping() {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	for range ticker.C {
		o.Collect()
	}
}

func (o *ErrorLogCollector) Collect() {
	metrics, err := o.scrapeAllErrorLogs()
	if err != nil {
		fmt.Println("failed collecting error log metrics:", err)
		return
	}

	o.Count.Reset()
	for _, metric := range metrics {
		o.Count.WithLabelValues(metric.Func, metric.Level, metric.Source, metric.Unit, metric.Msg).Set(metric.Count)
	}
}

func (o *ErrorLogCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range []prometheus.Collector{o.Count} {
		metric.Describe(ch)
	}
}
