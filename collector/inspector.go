package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type InspectorLayoutCollector struct {
	*CollectorOpts
	Volume *prometheus.GaugeVec
}

type InspectorAccessTimeVolumeCollector struct {
	*CollectorOpts
	Volume *prometheus.GaugeVec
}

type InspectorAccessTimeFilesCollector struct {
	*CollectorOpts
	Files *prometheus.GaugeVec
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorLayoutCollector(opts *CollectorOpts) *InspectorLayoutCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorLayoutCollector{
		CollectorOpts: opts,
		Volume: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_layout_volume_bytes",
				Help:        "volume per layout in bytes",
				ConstLabels: labels,
			},
			[]string{"layout", "type", "nominal_stripes", "blocksize"},
		),
	}
}

func (o *InspectorLayoutCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Volume,
	}
}

func (o *InspectorLayoutCollector) collectInspectorLayoutDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorLayout(context.Background(), "root")
	if err != nil {
		return err
	}

	o.Volume.Reset()

	for _, m := range mds {

		volume, err := strconv.ParseFloat(m.Volume, 64)
		if err == nil {
			o.Volume.WithLabelValues(m.Layout, m.Type, m.NominalStripes, m.BlockSize).Set(volume)
		}
	}

	return nil

} // collectInspectorLayoutDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorLayoutCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorLayoutCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorLayoutDF(); err != nil {
		log.Println("failed collecting eos inspector metrics:", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorAccessTimeVolumeCollector(opts *CollectorOpts) *InspectorAccessTimeVolumeCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorAccessTimeVolumeCollector{
		CollectorOpts: opts,
		Volume: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_accesstime_volume_bytes",
				Help:        "volume per access time in bytes",
				ConstLabels: labels,
			},
			[]string{"bin"},
		),
	}
}

func (o *InspectorAccessTimeVolumeCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Volume,
	}
}

func (o *InspectorAccessTimeVolumeCollector) collectInspectorAccessTimeVolumeDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorAccessTimeVolume(context.Background(), "root")
	if err != nil {
		return err
	}

	o.Volume.Reset()

	for _, m := range mds {

		volume, err := strconv.ParseFloat(m.Volume, 64)
		if err == nil {
			o.Volume.WithLabelValues(m.Bin).Set(volume)
		}
	}

	return nil

} // collectInspectorAccessTimeVolumeDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorAccessTimeVolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorAccessTimeVolumeCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorAccessTimeVolumeDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (accesstime volume):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorAccessTimeFilesCollector(opts *CollectorOpts) *InspectorAccessTimeFilesCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorAccessTimeFilesCollector{
		CollectorOpts: opts,
		Files: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_accesstime_files_bytes",
				Help:        "files per access time",
				ConstLabels: labels,
			},
			[]string{"bin"},
		),
	}
}

func (o *InspectorAccessTimeFilesCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Files,
	}
}

func (o *InspectorAccessTimeFilesCollector) collectInspectorAccessTimeFilesDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorAccessTimeFiles(context.Background(), "root")
	if err != nil {
		return err
	}

	o.Files.Reset()

	for _, m := range mds {

		files, err := strconv.ParseFloat(m.Files, 64)
		if err == nil {
			o.Files.WithLabelValues(m.Bin).Set(files)
		}
	}

	return nil

} // collectInspectorAccessTimeFilesDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorAccessTimeFilesCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorAccessTimeFilesCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorAccessTimeFilesDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (accestime files):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
