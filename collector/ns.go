package collector

import (
	"context"
	"log"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
	//"os"
	//"bufio"
	"fmt"
	//"strings"
)

type NSCollector struct {
	Boot_file_time					*prometheus.GaugeVec
	Boot_status					*prometheus.GaugeVec
	Boot_time					*prometheus.GaugeVec
	Cache_container_maxsize				*prometheus.GaugeVec
	Cache_container_occupancy			*prometheus.GaugeVec
	Cache_files_maxsize				*prometheus.GaugeVec
	Cache_files_occupancy				*prometheus.GaugeVec
	Fds_all						*prometheus.GaugeVec
	Fusex_activeclients				*prometheus.GaugeVec
	Fusex_caps					*prometheus.GaugeVec
	Fusex_clients					*prometheus.GaugeVec
	Fusex_lockedclients				*prometheus.GaugeVec
	Latency_dirs					*prometheus.GaugeVec
	Latency_files					*prometheus.GaugeVec
	Latency_pending_updates				*prometheus.GaugeVec
	Latencypeak_eosviewmutex_1min			*prometheus.GaugeVec
	Latencypeak_eosviewmutex_2min			*prometheus.GaugeVec
	Latencypeak_eosviewmutex_5min			*prometheus.GaugeVec
	Latencypeak_eosviewmutex_last			*prometheus.GaugeVec
	Memory_growth					*prometheus.GaugeVec
	Memory_resident					*prometheus.GaugeVec
	Memory_share					*prometheus.GaugeVec
	Memory_virtual					*prometheus.GaugeVec
	Stat_threads					*prometheus.GaugeVec
	Total_directories				*prometheus.GaugeVec
	Total_directories_changelog_avg_entry_size	*prometheus.GaugeVec
	Total_directories_changelog_size		*prometheus.GaugeVec
	Total_files					*prometheus.GaugeVec
	Total_files_changelog_avg_entry_size		*prometheus.GaugeVec
	Total_files_changelog_size			*prometheus.GaugeVec
	Uptime						*prometheus.GaugeVec
}

type NSActivityCollector struct {
	Sum		*prometheus.GaugeVec
	Last_5s		*prometheus.GaugeVec
	Last_60s	*prometheus.GaugeVec
	Last_300s	*prometheus.GaugeVec
	Last_3600s	*prometheus.GaugeVec
}

var Mds []*eosclient.NSInfo
var Mdsact []*eosclient.NSActivityInfo
var err error

func init() {
	Mds, Mdsact, err = getNSData()
	fmt.Println("Data initialized")
}

//NewNSCollector creates an instance of the NSCollector and instantiates
// the individual metrics that show information about the NS.
func NewNSCollector(cluster string) *NSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &NSCollector{
		Boot_file_time: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_boot_file_time_seconds",
				Help:        "Boot_file_time: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Boot_status: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_boot_status",
				Help:        "Boot_status: Shows '1' if it's booted and '0' if it's not.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Boot_time: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_boot_time_seconds",
				Help:        "Boot_time: Time to perform the last boot.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Cache_container_maxsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_cache_container_max_total",
				Help:        "Cache_container_maxsize: Max number of containers allowed in this namespace.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Cache_container_occupancy: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_cache_container_occ_total",
				Help:        "Cache_container_occupancy: Total number of containers occupied in cache.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Cache_files_maxsize: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_cache_files_total",
				Help:        "Cache_files_maxsize: Number of max cache files.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Cache_files_occupancy: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_cache_files_occ_total",
				Help:        "Cache_files_occupancy: Number of cache files occupied.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Fds_all: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_fds_total",
				Help:        "Fds_all: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Fusex_activeclients: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_fusex_activeclients_total",
				Help:        "Fusex_clients: Active FUSEX clients.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Fusex_caps: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_fusex_caps_total",
				Help:        "Fusex_caps: Current FUSEX caps performed.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Fusex_clients: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_fusex_clients_total",
				Help:        "Fusex_clients: Total FUSEX clients.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Fusex_lockedclients: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_fusex_locked_clients_total",
				Help:        "Fusex_lockedclients: Locked FUSEX clients.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latency_dirs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_dirs_seconds",
				Help:        "Latency_dirs: Directory latency in seconds.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latency_files: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_files_seconds",
				Help:        "Latency_files: Files' latency in seconds.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latency_pending_updates: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_pend_upd_seconds",
				Help:        "Latency_pending_updates:  Latency of pending updates is seconds.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latencypeak_eosviewmutex_1min: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_eosvm_1min_seconds",
				Help:        "Latencypeak_eosviewmutex_1min: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latencypeak_eosviewmutex_2min: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_eosvm_2min_seconds",
				Help:        "Latencypeak_eosviewmutex_2min: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latencypeak_eosviewmutex_5min: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_eosvm_5min_seconds",
				Help:        "Latencypeak_eosviewmutex_5min: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Latencypeak_eosviewmutex_last: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_lat_eosvm_last_seconds",
				Help:        "Latencypeak_eosviewmutex_last: TODO.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Memory_growth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_mem_growth_bytes",
				Help:        "Memory_growth: TODO in bytes.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Memory_resident: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_mem_res_bytes",
				Help:        "Memory_resident: Resident memory size in bytes.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Memory_share: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_mem_share_bytes",
				Help:        "Memory_share: Shared memory size in bytes.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Memory_virtual: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_mem_virt_bytes",
				Help:        "Memory_virtual: Virtual memory size in bytes.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Stat_threads: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_threads_total",
				Help:        "Stat_threads: Number of used threads.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_directories: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_dirs_total",
				Help:        "Total_directories: Number of directories present in this namespace.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_directories_changelog_avg_entry_size: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_dirs_clog_avg_entry_size_total",
				Help:        "Total_directories_changelog_avg_entry_size: TODO",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_directories_changelog_size: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_dirs_clog_size_total",
				Help:        "Total_directories_changelog_size: TODO",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_files: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_files_total",
				Help:        "Total_files: Total files residing in the namespace.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_files_changelog_avg_entry_size: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_files_clog_avg_entry_size_total",
				Help:        "Total_files_changelog_avg_entry_size: TODO",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Total_files_changelog_size: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_files_clog_size_total",
				Help:        "Total_files_changelog_size: TODO",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
		Uptime: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "ns_uptime_seconds",
				Help:        "Uptime: Time since the namespace was started last time in seconds.",
				ConstLabels: labels,
			},
			[]string{"whatever"},
		),
	}
}

//NewNSActivityCollector creates an instance of the NSActivityCollector and instantiates
// the individual metrics that show information about the NS activity.
func NewNSActivityCollector(cluster string) *NSActivityCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &NSActivityCollector{
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
		o.Boot_file_time,
		o.Boot_status,
		o.Boot_time,
		o.Cache_container_maxsize,
		o.Cache_container_occupancy,
		o.Cache_files_maxsize,
		o.Cache_files_occupancy,
		o.Fds_all,
		o.Fusex_activeclients,
		o.Fusex_caps,
		o.Fusex_clients,
		o.Fusex_lockedclients,
		o.Latency_dirs,
		o.Latency_files,
		o.Latency_pending_updates,
		o.Latencypeak_eosviewmutex_1min,
		o.Latencypeak_eosviewmutex_2min,
		o.Latencypeak_eosviewmutex_5min,
		o.Latencypeak_eosviewmutex_last,
		o.Memory_growth,
		o.Memory_resident,
		o.Memory_share,
		o.Memory_virtual,
		o.Stat_threads,
		o.Total_directories,
		o.Total_directories_changelog_avg_entry_size,
		o.Total_directories_changelog_size,
		o.Total_files,
		o.Total_files_changelog_avg_entry_size,
		o.Total_files_changelog_size,
		o.Uptime,
	}
}

func (o *NSActivityCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.Sum,
		o.Last_5s,
		o.Last_60s,
		o.Last_300s,
		o.Last_3600s,
	}
}

func getNSData() ([]*eosclient.NSInfo,[]*eosclient.NSActivityInfo, error) {
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

	return mds, mdsact, nil

}

func (o *NSCollector) collectNSDF() error {

	var boot_status float64
	for _, m := range Mds {

		// Boot_file_time

		boot_ft, err := strconv.ParseFloat(m.Boot_file_time, 64)
		if err == nil {
			o.Boot_file_time.WithLabelValues("a").Set(boot_ft)
		}

		// Boot_status

		switch stat := m.Boot_status; stat {
			case "booted":
				boot_status = 1
			default:
				boot_status = 0
		}

		o.Boot_status.WithLabelValues("a").Set(float64(boot_status))

		// Boot_time

		boot_time, err := strconv.ParseFloat(m.Boot_time, 64)
		if err == nil {
			o.Boot_time.WithLabelValues("a").Set(boot_time)
		}

		// Cache_container_maxsize

		cache_cont_max, err := strconv.ParseFloat(m.Cache_container_maxsize, 64)
		if err == nil {
			o.Cache_container_maxsize.WithLabelValues("a").Set(cache_cont_max)
		}

		// Cache_container_occupancy

		cache_cont_occ, err := strconv.ParseFloat(m.Cache_container_occupancy, 64)
		if err == nil {
			o.Cache_container_occupancy.WithLabelValues("a").Set(cache_cont_occ)
		}

		// Cache_files_maxsize

		cache_files_max, err := strconv.ParseFloat(m.Cache_files_maxsize, 64)
		if err == nil {
			o.Cache_files_maxsize.WithLabelValues("a").Set(cache_files_max)
		}

		// Cache_files_occupancy

		cache_files_occ, err := strconv.ParseFloat(m.Cache_files_occupancy, 64)
		if err == nil {
			o.Cache_files_occupancy.WithLabelValues("a").Set(cache_files_occ)
		}

		// Fds_all

		fds_all, err := strconv.ParseFloat(m.Fds_all, 64)
		if err == nil {
			o.Fds_all.WithLabelValues("a").Set(fds_all)
		}

		// Fusex_activeclients

		fusex_actclients, err := strconv.ParseFloat(m.Fusex_activeclients, 64)
		if err == nil {
			o.Fusex_activeclients.WithLabelValues("a").Set(fusex_actclients)
		}

		// Fusex_caps

		fusex_caps, err := strconv.ParseFloat(m.Fusex_caps, 64)
		if err == nil {
			o.Fusex_caps.WithLabelValues("a").Set(fusex_caps)
		}

		// Fusex_clients

		fusex_clients, err := strconv.ParseFloat(m.Fusex_clients, 64)
		if err == nil {
			o.Fusex_clients.WithLabelValues("a").Set(fusex_clients)
		}

		// Fusex_lockedclients

		fusex_lockedcs, err := strconv.ParseFloat(m.Fusex_lockedclients, 64)
		if err == nil {
			o.Fusex_lockedclients.WithLabelValues("a").Set(fusex_lockedcs)
		}

		// Latency_dirs

		lat_dirs, err := strconv.ParseFloat(m.Latency_dirs, 64)
		if err == nil {
			o.Latency_dirs.WithLabelValues("a").Set(lat_dirs)
		}

		// Latency_files

		lat_files, err := strconv.ParseFloat(m.Latency_files, 64)
		if err == nil {
			o.Latency_files.WithLabelValues("a").Set(lat_files)
		}

		// Latency_pending_updates

		lat_pen_upd, err := strconv.ParseFloat(m.Latency_pending_updates, 64)
		if err == nil {
			o.Latency_pending_updates.WithLabelValues("a").Set(lat_pen_upd)
		}

		// Latencypeak_eosviewmutex_1min

		lat_eosvm_1m, err := strconv.ParseFloat(m.Latencypeak_eosviewmutex_1min, 64)
		if err == nil {
			o.Latencypeak_eosviewmutex_1min.WithLabelValues("a").Set(lat_eosvm_1m)
		}

		// Latencypeak_eosviewmutex_2min

		lat_eosvm_2m, err := strconv.ParseFloat(m.Latencypeak_eosviewmutex_2min, 64)
		if err == nil {
			o.Latencypeak_eosviewmutex_2min.WithLabelValues("a").Set(lat_eosvm_2m)
		}

		// Latencypeak_eosviewmutex_5min

		lat_eosvm_5m, err := strconv.ParseFloat(m.Latencypeak_eosviewmutex_5min, 64)
		if err == nil {
			o.Latencypeak_eosviewmutex_5min.WithLabelValues("a").Set(lat_eosvm_5m)
		}

		// Latencypeak_eosviewmutex_last

		lat_eosvm_last, err := strconv.ParseFloat(m.Latencypeak_eosviewmutex_last, 64)
		if err == nil {
			o.Latencypeak_eosviewmutex_last.WithLabelValues("a").Set(lat_eosvm_last)
		}

		// Memory_growth

		mem_growth, err := strconv.ParseFloat(m.Memory_growth, 64)
		if err == nil {
			o.Memory_growth.WithLabelValues("a").Set(mem_growth)
		}

		// Memory_resident

		mem_res, err := strconv.ParseFloat(m.Memory_resident, 64)
		if err == nil {
			o.Memory_resident.WithLabelValues("a").Set(mem_res)
		}

		// Memory_share
		mem_share, err := strconv.ParseFloat(m.Memory_share, 64)
		if err == nil {
			o.Memory_share.WithLabelValues("a").Set(mem_share)
		}

		// Memory_virtual

		mem_virt, err := strconv.ParseFloat(m.Memory_virtual, 64)
		if err == nil {
			o.Memory_virtual.WithLabelValues("a").Set(mem_virt)
		}

		// Stat_threads

		stat_threads, err := strconv.ParseFloat(m.Stat_threads, 64)
		if err == nil {
			o.Stat_threads.WithLabelValues("a").Set(stat_threads)
		}

		// Total_directories

		total_dirs, err := strconv.ParseFloat(m.Total_directories, 64)
		if err == nil {
			o.Total_directories.WithLabelValues("a").Set(total_dirs)
		}

		// Total_directories_changelog_avg_entry_size
		total_dirs_clog_avg_entry_size, err := strconv.ParseFloat(m.Total_directories_changelog_avg_entry_size, 64)

		if err == nil {
			o.Total_directories_changelog_avg_entry_size.WithLabelValues("a").Set(total_dirs_clog_avg_entry_size,)
		}

		// Total_directories_changelog_size

		total_dirs_clog_size, err := strconv.ParseFloat(m.Total_directories_changelog_size, 64)
		if err == nil {
			o.Total_directories_changelog_size.WithLabelValues("a").Set(total_dirs_clog_size)
		}

		// Total_files

		total_files, err := strconv.ParseFloat(m.Total_files, 64)
		if err == nil {
			o.Total_files.WithLabelValues("a").Set(total_files)
		}

		// Total_files_changelog_avg_entry_size

		total_files_clog_avg_entry_size, err := strconv.ParseFloat(m.Total_files_changelog_avg_entry_size, 64)
		if err == nil {
			o.Total_files_changelog_avg_entry_size.WithLabelValues("a").Set(total_files_clog_avg_entry_size)
		}

		// Total_files_changelog_size

		total_files_clog_size, err := strconv.ParseFloat(m.Total_files_changelog_size, 64)
		if err == nil {
			o.Total_files_changelog_size.WithLabelValues("a").Set(total_files_clog_size)
		}

		// Uptime

		uptime, err := strconv.ParseFloat(m.Uptime, 64)
		if err == nil {
			o.Uptime.WithLabelValues("a").Set(uptime)
		}
	}

	return nil

} // collectNSDF()

func (o *NSActivityCollector) collectNSActivityDF() error {

	for _, n := range Mdsact{
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

} // collectNSActivityDF()

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

// Describe sends the descriptors of each NSActivityCollector related metrics we have defined
func (o *NSActivityCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *NSActivityCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectNSActivityDF(); err != nil {
		log.Println("failed collecting space metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
