package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type FsckCollector struct {
	Count *prometheus.GaugeVec
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewFsckCollector(cluster string) *FsckCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &FsckCollector{
		Count: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fsck_stat",
				Help:        "fsck inconsistency report: eos fsck stat",
				ConstLabels: labels,
			},
			[]string{"tag"},
		),
	}
}

func (o *FsckCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Count,
	}
}

// func getEOSInstance() string {
// 	// Get the EOS cluster name from MGM's filesystem
// 	var str string

// 	file, err := os.Open("/etc/sysconfig/eos_env")
// 	if err != nil {
// 		fmt.Println(err)
// 	}
// 	defer file.Close()

// 	scanner := bufio.NewScanner(file)
// 	for scanner.Scan() {
// 		l := scanner.Text()
// 		if strings.HasPrefix(l, "EOS_MGM_ALIAS=") {
// 			s := strings.Split(l, "EOS_MGM_ALIAS=")
// 			str = strings.Replace(s[1], "\"", "", -1)
// 		}
// 	}

// 	if err := scanner.Err(); err != nil {
// 		fmt.Println(err)
// 	}

// 	return str
// }

func (o *FsckCollector) collectFsckDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.FsckReport(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	o.Count.Reset()

	for _, m := range mds {

		count, err := strconv.ParseFloat(m.Count, 64)
		if err == nil {
			o.Count.WithLabelValues(m.Tag).Set(count)
		}
	}

	return nil

} // collectFsckDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *FsckCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *FsckCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectFsckDF(); err != nil {
		log.Println("failed collecting fsck metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
