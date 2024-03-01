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

type InspectorBirthTimeVolumeCollector struct {
	*CollectorOpts
	Volume *prometheus.GaugeVec
}

type InspectorBirthTimeFilesCollector struct {
	*CollectorOpts
	Files *prometheus.GaugeVec
}

type InspectorGroupCostDiskCollector struct {
	*CollectorOpts
	Cost *prometheus.GaugeVec
}

type InspectorGroupCostDiskTBYearsCollector struct {
	*CollectorOpts
	TBYears *prometheus.GaugeVec
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
				Name:        "inspector_accesstime_files",
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

// Birthtime

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorBirthTimeVolumeCollector(opts *CollectorOpts) *InspectorBirthTimeVolumeCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorBirthTimeVolumeCollector{
		CollectorOpts: opts,
		Volume: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_birthtime_volume_bytes",
				Help:        "volume per birth time in bytes",
				ConstLabels: labels,
			},
			[]string{"bin"},
		),
	}
}

func (o *InspectorBirthTimeVolumeCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Volume,
	}
}

func (o *InspectorBirthTimeVolumeCollector) collectInspectorBirthTimeVolumeDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorBirthTimeVolume(context.Background(), "root")
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

} // collectInspectorBirthTimeVolumeDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorBirthTimeVolumeCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorBirthTimeVolumeCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorBirthTimeVolumeDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (birthtime volume):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorBirthTimeFilesCollector(opts *CollectorOpts) *InspectorBirthTimeFilesCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorBirthTimeFilesCollector{
		CollectorOpts: opts,
		Files: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_birhttime_files",
				Help:        "files per birth time",
				ConstLabels: labels,
			},
			[]string{"bin"},
		),
	}
}

func (o *InspectorBirthTimeFilesCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Files,
	}
}

func (o *InspectorBirthTimeFilesCollector) collectInspectorBirthTimeFilesDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorBirthTimeFiles(context.Background(), "root")
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

} // collectInspectorBirthTimeFilesDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorBirthTimeFilesCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorBirthTimeFilesCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorBirthTimeFilesDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (accestime files):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// Group Cost

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorGroupCostDiskCollector(opts *CollectorOpts) *InspectorGroupCostDiskCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorGroupCostDiskCollector{
		CollectorOpts: opts,
		Cost: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_group_cost_disk",
				Help:        "cost per group",
				ConstLabels: labels,
			},
			[]string{"groupname", "price"},
		),
	}
}

func (o *InspectorGroupCostDiskCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Cost,
	}
}

func (o *InspectorGroupCostDiskCollector) collectInspectorGroupCostDiskDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorGroupCostDisk(context.Background(), "root")
	if err != nil {
		return err
	}

	o.Cost.Reset()

	for _, m := range mds {

		files, err := strconv.ParseFloat(m.Cost, 64)
		if err == nil {
			o.Cost.WithLabelValues(m.Groupname, m.Price).Set(files)
		}
	}

	return nil

} // collectInspectorGroupCostDiskDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorGroupCostDiskCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorGroupCostDiskCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorGroupCostDiskDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (group cost disk):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewInspectorGroupCostDiskTBYearsCollector(opts *CollectorOpts) *InspectorGroupCostDiskTBYearsCollector {
	cluster := opts.Cluster
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &InspectorGroupCostDiskTBYearsCollector{
		CollectorOpts: opts,
		TBYears: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "inspector_group_cost_disk_tbyears",
				Help:        "tbyears per group",
				ConstLabels: labels,
			},
			[]string{"groupname"},
		),
	}
}

func (o *InspectorGroupCostDiskTBYearsCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.TBYears,
	}
}

func (o *InspectorGroupCostDiskTBYearsCollector) collectInspectorGroupCostDiskTBYearsDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url, Timeout: o.Timeout}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListInspectorGroupCostDiskTBYears(context.Background(), "root")
	if err != nil {
		return err
	}

	o.TBYears.Reset()

	for _, m := range mds {

		files, err := strconv.ParseFloat(m.TBYears, 64)
		if err == nil {
			o.TBYears.WithLabelValues(m.Groupname).Set(files)
		}
	}

	return nil

} // collectInspectorGroupCostDiskTBYearsDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *InspectorGroupCostDiskTBYearsCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *InspectorGroupCostDiskTBYearsCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectInspectorGroupCostDiskTBYearsDF(); err != nil {
		log.Println("failed collecting eos inspector metrics (group cost disk tbyears):", err)
		return
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
