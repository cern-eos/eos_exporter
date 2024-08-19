package collector

import (
	"context"
	"fmt"

	// "time"
	"log"

	"github.com/cern-eos/eos_exporter/eosclient"
	"github.com/prometheus/client_golang/prometheus"
)

type QuotasCollector struct {
	*CollectorOpts
	QuotaUsedBytes        *prometheus.GaugeVec
	QuotaMaxBytes         *prometheus.GaugeVec
	QuotaUsedLogicalBytes *prometheus.GaugeVec
	QuotaMaxLogicalBytes  *prometheus.GaugeVec
	QuotaUsedFiles        *prometheus.GaugeVec
	QuotaMaxFiles         *prometheus.GaugeVec
}

// NewQuotasCollector creates an cluster of the QuotasCollector
func NewQuotasCollector(opts *CollectorOpts) *QuotasCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	namespace := "eos"

	return &QuotasCollector{
		//file: f,
		CollectorOpts: opts,
		QuotaUsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_used_bytes",
				Help:        "Quota used bytes",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
		QuotaMaxBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_max_bytes",
				Help:        "Quota max bytes",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
		QuotaUsedLogicalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_used_logical_bytes",
				Help:        "Quota used logical bytes",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
		QuotaMaxLogicalBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_max_logical_bytes",
				Help:        "Quota maxlogical bytes",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
		QuotaUsedFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_used_files",
				Help:        "Quota used files",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
		QuotaMaxFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "quota_max_files",
				Help:        "Quota max files",
				ConstLabels: labels,
			},
			[]string{"uid", "gid", "space"},
		),
	}
}

func (o *QuotasCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.QuotaUsedBytes,
		o.QuotaMaxBytes,
		o.QuotaUsedFiles,
		o.QuotaMaxFiles,
		o.QuotaUsedLogicalBytes,
		o.QuotaMaxLogicalBytes,
	}
}

func (o *QuotasCollector) collectQuotaDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	quotas, err := client.Quotas(context.Background(), "root")
	if err != nil {
		return err
	}

	// a GaugeVector keeps its state for any combination of labels until the process is restarted.
	// For metrics obtained from systems like EOS that do not produce a complete set of metrics (only the active metrics)
	// then we risk to expose these metrics forever until the next process restart.
	// To workaround this, we reset the gauge vectors when collecting metrics.

	// output is like this:
	// quota=node uid=9218 space=/eos/user/ usedbytes=158090138 usedlogicalbytes=79045069 usedfiles=1546 maxbytes=0 maxlogicalbytes=0 maxfiles=0 percentageusedbytes=100.00 statusbytes=ignored statusfiles=ignored

	o.QuotaUsedBytes.Reset()
	o.QuotaMaxBytes.Reset()
	o.QuotaUsedLogicalBytes.Reset()
	o.QuotaMaxLogicalBytes.Reset()
	o.QuotaUsedFiles.Reset()
	o.QuotaMaxFiles.Reset()

	for _, q := range quotas {
		o.QuotaUsedBytes.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.UsedBytes))
		o.QuotaMaxBytes.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.MaxBytes))
		o.QuotaUsedLogicalBytes.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.UsedLogicalBytes))
		o.QuotaMaxLogicalBytes.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.MaxLogicalBytes))
		o.QuotaUsedFiles.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.UsedFiles))
		o.QuotaMaxFiles.WithLabelValues(q.Uid, q.Gid, q.Space).Set(float64(q.MaxFiles))
	}

	return nil

} // collectQuotaDF()

// Describe sends the descriptors of each SpaceCollector related metrics we have defined
func (o *QuotasCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		fmt.Print(metric)
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *QuotasCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectQuotaDF(); err != nil {
		log.Println("failed collecting quota  metrics:", err)
		return
	}

	for _, collector := range o.collectorList() {
		collector.Collect(ch)
	}
}
