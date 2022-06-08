package collector

import (
	"context"
	"fmt"
	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
	"log"
	"strconv"
)

type IOInfoCollector struct {
	Total_bwd_seeks          *prometheus.GaugeVec
	Total_bytes_bwd_wseek    *prometheus.GaugeVec
	Total_bytes_deleted      *prometheus.GaugeVec
	Total_bytes_fwd_seek     *prometheus.GaugeVec
	Total_bytes_read         *prometheus.GaugeVec
	Total_bytes_written      *prometheus.GaugeVec
	Total_bytes_xl_bwd_wseek *prometheus.GaugeVec
	Total_bytes_xl_fwd_seek  *prometheus.GaugeVec
	Total_disk_time_read     *prometheus.GaugeVec
	Total_disk_time_write    *prometheus.GaugeVec
	Total_files_deleted      *prometheus.GaugeVec
	Total_fwd_seeks          *prometheus.GaugeVec
	Total_read_calls         *prometheus.GaugeVec
	Total_readv_calls        *prometheus.GaugeVec
	Total_write_calls        *prometheus.GaugeVec
	Total_xl_bwd_seeks       *prometheus.GaugeVec
	Total_xl_fwd_seeks       *prometheus.GaugeVec
	//Measurement *prometheus.GaugeVec
	//Last_60s    *prometheus.GaugeVec
	//Last_300s   *prometheus.GaugeVec
	//Last_3600s  *prometheus.GaugeVec
	//Last_86400s *prometheus.GaugeVec
}

type IOAppInfoCollector struct {
	Total_in  *prometheus.GaugeVec
	Total_out *prometheus.GaugeVec
	//Last_60s    *prometheus.GaugeVec
	//Last_300s   *prometheus.GaugeVec
	//Last_3600s  *prometheus.GaugeVec
	//Last_86400s *prometheus.GaugeVec
}

//NewIOInfoCollector creates an cluster of the IOInfoCollector
func NewIOInfoCollector(cluster string) *IOInfoCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &IOInfoCollector{

		Total_bwd_seeks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bwd_seeks_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_bwd_wseek: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_bwd_wseek_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_deleted: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_deleted_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_fwd_seek: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_fwd_seek_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_read: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_read_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_written: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_written_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_xl_bwd_wseek: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_xl_bwd_wseek_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_bytes_xl_fwd_seek: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_bytes_xl_fwd_seek_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_disk_time_read: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_disk_time_read_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_disk_time_write: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_disk_time_write_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_files_deleted: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_files_deleted_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_fwd_seeks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_fwd_seeks_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_read_calls: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_read_calls_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_readv_calls: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_readv_calls_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_write_calls: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_write_calls_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_xl_bwd_seeks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_xl_bwd_seeks_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
		Total_xl_fwd_seeks: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_xl_fwd_seeks_total",
				Help:        "IO Stat Total",
				ConstLabels: labels,
			},
			[]string{},
		),
	}
}

//NewIOAppInfoCollector creates an cluster of the IOAppInfoCollector
func NewIOAppInfoCollector(cluster string) *IOAppInfoCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &IOAppInfoCollector{

		Total_in: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_app_in_bytes",
				Help:        "In IO by app",
				ConstLabels: labels,
			},
			[]string{"app"},
		),
		Total_out: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "io_app_out_bytes",
				Help:        "Out IO by app",
				ConstLabels: labels,
			},
			[]string{"app"},
		),
	}
}

func (o *IOInfoCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Total_bwd_seeks,
		o.Total_bytes_bwd_wseek,
		o.Total_bytes_deleted,
		o.Total_bytes_fwd_seek,
		o.Total_bytes_read,
		o.Total_bytes_written,
		o.Total_bytes_xl_bwd_wseek,
		o.Total_bytes_xl_fwd_seek,
		o.Total_disk_time_read,
		o.Total_disk_time_write,
		o.Total_files_deleted,
		o.Total_fwd_seeks,
		o.Total_read_calls,
		o.Total_readv_calls,
		o.Total_write_calls,
		o.Total_xl_bwd_seeks,
		o.Total_xl_fwd_seeks,
	}
}

func (o *IOAppInfoCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Total_in,
		o.Total_out,
	}
}

func (o *IOInfoCollector) collectIOInfoDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		fmt.Println("Panic error while getting new eosclient: ", err)
		panic(err)
	}

	mds, err := client.ListIOInfo(context.Background())
	if err != nil {
		fmt.Println("Panic error while ListIOInfo: ", err)
		for _, m := range mds {
			fmt.Println("Measurement: ", m.Measurement)
		}
		panic(err)
	}

	for _, m := range mds {
		if m.Measurement == "bwd_seeks" {
			total_bwd_seeks, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bwd_seeks.WithLabelValues().Set(total_bwd_seeks)
			}
		}
		if m.Measurement == "bytes_bwd_wseek" {
			total_bytes_bwd_wseek, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_bwd_wseek.WithLabelValues().Set(total_bytes_bwd_wseek)
			}
		}
		if m.Measurement == "bytes_deleted" {
			total_bytes_deleted, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_deleted.WithLabelValues().Set(total_bytes_deleted)
			}
		}
		if m.Measurement == "bytes_fwd_seek" {
			total_bytes_fwd_seek, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_fwd_seek.WithLabelValues().Set(total_bytes_fwd_seek)
			}
		}
		if m.Measurement == "bytes_read" {
			total_bytes_read, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_read.WithLabelValues().Set(total_bytes_read)
			}
		}
		if m.Measurement == "bytes_written" {
			total_bytes_written, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_written.WithLabelValues().Set(total_bytes_written)
			}
		}
		if m.Measurement == "bytes_xl_fwd_seek" {
			total_bytes_xl_fwd_seek, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_bytes_xl_fwd_seek.WithLabelValues().Set(total_bytes_xl_fwd_seek)
			}
		}
		if m.Measurement == "disk_time_read" {
			total_disk_time_read, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_disk_time_read.WithLabelValues().Set(total_disk_time_read)
			}
		}
		if m.Measurement == "disk_time_write" {
			total_disk_time_write, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_disk_time_write.WithLabelValues().Set(total_disk_time_write)
			}
		}
		if m.Measurement == "files_deleted" {
			total_files_deleted, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_files_deleted.WithLabelValues().Set(total_files_deleted)
			}
		}
		if m.Measurement == "fwd_seeks" {
			total_fwd_seeks, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_fwd_seeks.WithLabelValues().Set(total_fwd_seeks)
			}
		}
		if m.Measurement == "read_calls" {
			total_read_calls, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_read_calls.WithLabelValues().Set(total_read_calls)
			}
		}
		if m.Measurement == "readv_calls" {
			total_readv_calls, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_readv_calls.WithLabelValues().Set(total_readv_calls)
			}
		}
		if m.Measurement == "write_calls" {
			total_write_calls, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_write_calls.WithLabelValues().Set(total_write_calls)
			}
		}
		if m.Measurement == "xl_bwd_seeks" {
			total_xl_bwd_seeks, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_xl_bwd_seeks.WithLabelValues().Set(total_xl_bwd_seeks)
			}
		}
		if m.Measurement == "xl_fwd_seeks" {
			total_xl_fwd_seeks, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_xl_fwd_seeks.WithLabelValues().Set(total_xl_fwd_seeks)
			}
		}
	}

	return nil

} // collectIOInfoDF()

func (o *IOAppInfoCollector) collectIOAppInfoDF() error {
	ins := getEOSInstance()
	url := "root://" + ins + ".cern.ch"
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListIOAppInfo(context.Background())
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		if m.Measurement == "app_io_in" {
			total_in, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_in.WithLabelValues(m.Application).Set(total_in)
			}
		}
		if m.Measurement == "app_io_out" {
			total_out, err := strconv.ParseFloat(m.Total, 64)
			if err == nil {
				o.Total_out.WithLabelValues(m.Application).Set(total_out)
			}
		}
	}

	return nil

} // collectIOAppInfoDF()

// Describe sends the descriptors of each IOInfoCollector related metrics we have defined
func (o *IOInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *IOInfoCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectIOInfoDF(); err != nil {
		log.Println("failed collecting IO info metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// Describe sends the descriptors of each IOInfoCollector related metrics we have defined
func (o *IOAppInfoCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *IOAppInfoCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectIOAppInfoDF(); err != nil {
		log.Println("failed collecting IO info metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
