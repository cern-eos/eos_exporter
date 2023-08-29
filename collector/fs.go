package collector

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"gitlab.cern.ch/rvalverd/eos_exporter/eosclient"
)

type FSCollector struct {
	Host                       *prometheus.GaugeVec
	Port                       *prometheus.GaugeVec
	Id                         *prometheus.GaugeVec
	Uuid                       *prometheus.GaugeVec
	Path                       *prometheus.GaugeVec
	Schedgroup                 *prometheus.GaugeVec
	StatBoot                   *prometheus.GaugeVec
	Configstatus               *prometheus.GaugeVec
	Headroom                   *prometheus.GaugeVec
	StatErrc                   *prometheus.GaugeVec
	StatErrmsg                 *prometheus.GaugeVec
	StatDiskLoad               *prometheus.GaugeVec
	StatDiskReadratemb         *prometheus.GaugeVec
	StatDiskWriteratemb        *prometheus.GaugeVec
	StatNetEthratemib          *prometheus.GaugeVec
	StatNetInratemib           *prometheus.GaugeVec
	StatNetOutratemib          *prometheus.GaugeVec
	StatRopen                  *prometheus.GaugeVec
	StatWopen                  *prometheus.GaugeVec
	StatStatfsFreebytes        *prometheus.GaugeVec
	StatStatfsUsedbytes        *prometheus.GaugeVec
	StatStatfsCapacity         *prometheus.GaugeVec
	StatUsedfiles              *prometheus.GaugeVec
	StatStatfsFfree            *prometheus.GaugeVec
	StatStatfsFused            *prometheus.GaugeVec
	StatStatfsFiles            *prometheus.GaugeVec
	Drainstatus                *prometheus.GaugeVec
	StatDrainprogress          *prometheus.GaugeVec
	StatDrainfiles             *prometheus.GaugeVec
	StatDrainbytesleft         *prometheus.GaugeVec
	StatDrainretry             *prometheus.GaugeVec
	StatDrainFailed            *prometheus.GaugeVec
	Graceperiod                *prometheus.GaugeVec
	StatTimeleft               *prometheus.GaugeVec
	StatActive                 *prometheus.GaugeVec
	StatBalancerRunning        *prometheus.GaugeVec
	StatDrainerRunning         *prometheus.GaugeVec
	StatDiskIops               *prometheus.GaugeVec
	StatDiskBw                 *prometheus.GaugeVec
	StatGeotag                 *prometheus.GaugeVec
	StatHealth                 *prometheus.GaugeVec
	StatHealthRedundancyFactor *prometheus.GaugeVec
	StatHealthDrivesFailed     *prometheus.GaugeVec
	StatHealthDrivesTotal      *prometheus.GaugeVec
	StatHealthIndicator        *prometheus.GaugeVec
}

// NewFSCollector creates an cluster of the FSCollector and instantiates
// the individual metrics that show information about the FS.
func NewFSCollector(cluster string) *FSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	namespace := "eos"
	return &FSCollector{
		StatBoot: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_boot_status",
				Help:        "FS Status 0=booted, 1=booting, 2=bootfailure, 3=opserror, 4=down",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		Configstatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_config_status",
				Help:        "Configstatus: 0=rw,1=ro,2=drain,3=empty",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDiskLoad: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_load",
				Help:        "FS disk load",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDiskReadratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_readratemb",
				Help:        "FS stat Disk Read Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDiskWriteratemb: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_writeratemb",
				Help:        "FS Stat Disk Write Rate in MB/s",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatNetEthratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_net_ethratemib",
				Help:        "FS Stat Net Eth Rate in MiB/s",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatNetInratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_net_inratemib",
				Help:        "FS Stat Net In Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatNetOutratemib: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_net_outratemib",
				Help:        "FS Stat Net Out Rate MiB/s",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatRopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_ropen",
				Help:        "FS Open reads",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatWopen: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_wopen",
				Help:        "FS Open writes",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsUsedbytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_usedbytes",
				Help:        "FS StatFs Used Bytes",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsFreebytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_freebytes",
				Help:        "FS StatFs Free Bytes",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsCapacity: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_sizebytes",
				Help:        "FS StatFs Capacity",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsFused: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_usedfiles",
				Help:        "FS Used Files",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsFfree: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_freefiles",
				Help:        "FS Free-Files",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatStatfsFiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_statfs_totalfiles",
				Help:        "FS Files",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		Drainstatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_status",
				Help:        "FS Drain status: 0=nodrain,1=drained,2=draining,3=stalling,4=expired",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainprogress: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_progress",
				Help:        "FS Drain progress %",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainfiles: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_filesleft",
				Help:        "FS Drain files left",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainbytesleft: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_bytesleft",
				Help:        "FS Drain bytes left",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainretry: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_retries",
				Help:        "FS Drain retries",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainFailed: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_failed",
				Help:        "FS Drain failed",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatActive: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_status",
				Help:        "Status of fs: 0=offline,1=online",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatBalancerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_balancer_running",
				Help:        "FS Stat Balancer Running",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDrainerRunning: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_drain_running",
				Help:        "FS Stat Drainer Running",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDiskIops: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_iops",
				Help:        "FS Stat Disk IOPS",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatDiskBw: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_disk_bw_MB",
				Help:        "FS Stat Disk BW MB/Sec",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
		StatHealth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   namespace,
				Name:        "fs_health",
				Help:        "FS Stat Health: 0=OK,1=other",
				ConstLabels: labels,
			},
			[]string{"fs", "node"},
		),
	}
}

func (o *FSCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.StatBoot,
		o.Configstatus,
		o.StatDiskLoad,
		o.StatDiskReadratemb,
		o.StatDiskWriteratemb,
		o.StatNetEthratemib,
		o.StatNetInratemib,
		o.StatNetOutratemib,
		o.StatRopen,
		o.StatWopen,
		o.StatStatfsFreebytes,
		o.StatStatfsUsedbytes,
		o.StatStatfsCapacity,
		o.StatStatfsFfree,
		o.StatStatfsFused,
		o.StatStatfsFiles,
		o.Drainstatus,
		o.StatDrainprogress,
		o.StatDrainfiles,
		o.StatDrainbytesleft,
		o.StatDrainretry,
		o.StatDrainFailed,
		o.StatActive,
		o.StatBalancerRunning,
		o.StatDrainerRunning,
		o.StatDiskIops,
		o.StatDiskBw,
		o.StatHealth,
	}
}

func getEOSInstance() string {
	// Get the EOS cluster name from MGM's filesystem
	var str string

	file, err := os.Open("/etc/sysconfig/eos_env")
	if err != nil {
		fmt.Println(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		l := scanner.Text()
		if strings.HasPrefix(l, "EOS_MGM_ALIAS=") {
			s := strings.Split(l, "EOS_MGM_ALIAS=")
			str = strings.Replace(s[1], "\"", "", -1)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println(err)
	}

	return str
}

func (o *FSCollector) collectFSDF() error {
	ins := getEOSInstance()
	url := "root://" + ins
	opt := &eosclient.Options{URL: url}
	client, err := eosclient.New(opt)
	if err != nil {
		panic(err)
	}

	mds, err := client.ListFS(context.Background(), "root")
	if err != nil {
		panic(err)
	}

	for _, m := range mds {

		// Boot Status

		boot_status := 0
		switch stat := m.StatBoot; stat {
		case "booted":
			boot_status = 0
		case "booting":
			boot_status = 1
		case "bootfailure":
			boot_status = 2
		case "opserror":
			boot_status = 3
		case "down":
			boot_status = 4
		default:
			boot_status = 4
		}

		o.StatBoot.Reset()
		o.StatBoot.WithLabelValues(m.Id, m.Host).Set(float64(boot_status))

		// Config Status

		config_status := 0
		switch stat := m.Configstatus; stat {
		case "rw":
			config_status = 0
		case "ro":
			config_status = 1
		case "drain":
			config_status = 2
		case "empty":
			config_status = 3
		default:
			config_status = 0
		}

		o.Configstatus.Reset()
		o.Configstatus.WithLabelValues(m.Id, m.Host).Set(float64(config_status))

		diskload, err := strconv.ParseFloat(m.StatDiskLoad, 64)
		if err == nil {
			o.StatDiskLoad.Reset()
			o.StatDiskLoad.WithLabelValues(m.Id, m.Host).Set(diskload)
		}

		diskr, err := strconv.ParseFloat(m.StatDiskReadratemb, 64)
		if err == nil {
			o.StatDiskReadratemb.Reset()
			o.StatDiskReadratemb.WithLabelValues(m.Id, m.Host).Set(diskr)
		}

		diskw, err := strconv.ParseFloat(m.StatDiskWriteratemb, 64)
		if err == nil {
			o.StatDiskWriteratemb.Reset()
			o.StatDiskWriteratemb.WithLabelValues(m.Id, m.Host).Set(diskw)
		}

		ethrate, err := strconv.ParseFloat(m.StatNetEthratemib, 64)
		if err == nil {
			o.StatNetEthratemib.Reset()
			o.StatNetEthratemib.WithLabelValues(m.Id, m.Host).Set(ethrate)
		}

		inrate, err := strconv.ParseFloat(m.StatNetInratemib, 64)
		if err == nil {
			o.StatNetInratemib.Reset()
			o.StatNetInratemib.WithLabelValues(m.Id, m.Host).Set(inrate)
		}

		outrate, err := strconv.ParseFloat(m.StatNetOutratemib, 64)
		if err == nil {
			o.StatNetOutratemib.Reset()
			o.StatNetOutratemib.WithLabelValues(m.Id, m.Host).Set(outrate)
		}

		ropen, err := strconv.ParseFloat(m.StatRopen, 64)
		if err == nil {
			o.StatRopen.Reset()
			o.StatRopen.WithLabelValues(m.Id, m.Host).Set(ropen)
		}

		wopen, err := strconv.ParseFloat(m.StatWopen, 64)
		if err == nil {
			o.StatWopen.Reset()
			o.StatWopen.WithLabelValues(m.Id, m.Host).Set(wopen)
		}

		usedb, err := strconv.ParseFloat(m.StatStatfsUsedbytes, 64)
		if err == nil {
			o.StatStatfsUsedbytes.Reset()
			o.StatStatfsUsedbytes.WithLabelValues(m.Id, m.Host).Set(usedb)
		}

		fbytes, err := strconv.ParseFloat(m.StatStatfsFreebytes, 64)
		if err == nil {
			o.StatStatfsFreebytes.Reset()
			o.StatStatfsFreebytes.WithLabelValues(m.Id, m.Host).Set(fbytes)
		}

		fscap, err := strconv.ParseFloat(m.StatStatfsCapacity, 64)
		if err == nil {
			o.StatStatfsCapacity.Reset()
			o.StatStatfsCapacity.WithLabelValues(m.Id, m.Host).Set(fscap)
		}

		ufiles, err := strconv.ParseFloat(m.StatStatfsFused, 64)
		if err == nil {
			o.StatStatfsFused.Reset()
			o.StatStatfsFused.WithLabelValues(m.Id, m.Host).Set(ufiles)
		}

		ffree, err := strconv.ParseFloat(m.StatStatfsFfree, 64)
		if err == nil {
			o.StatStatfsFfree.Reset()
			o.StatStatfsFfree.WithLabelValues(m.Id, m.Host).Set(ffree)
		}

		files, err := strconv.ParseFloat(m.StatStatfsFiles, 64)
		if err == nil {
			o.StatStatfsFiles.Reset()
			o.StatStatfsFiles.WithLabelValues(m.Id, m.Host).Set(files)
		}

		// Drain Status.

		drain_status := 0
		switch stat := m.Drainstatus; stat {
		case "nodrain":
			drain_status = 0
		case "drained":
			drain_status = 1
		case "draining":
			drain_status = 2
		case "stalling":
			drain_status = 3
		case "expired":
			drain_status = 4
		default:
			drain_status = 0
		}

		o.Drainstatus.Reset()
		o.Drainstatus.WithLabelValues(m.Id, m.Host).Set(float64(drain_status))

		balr, err := strconv.ParseFloat(m.StatBalancerRunning, 64)
		if err == nil {
			o.StatBalancerRunning.Reset()
			o.StatBalancerRunning.WithLabelValues(m.Id, m.Host).Set(balr)
		}

		drainr, err := strconv.ParseFloat(m.StatDrainerRunning, 64)
		if err == nil {
			o.StatDrainerRunning.Reset()
			o.StatDrainerRunning.WithLabelValues(m.Id, m.Host).Set(drainr)
		}

		drainretry, err := strconv.ParseFloat(m.StatDrainretry, 64)
		if err == nil {
			o.StatDrainretry.Reset()
			o.StatDrainretry.WithLabelValues(m.Id, m.Host).Set(drainretry)
		}

		drainfailed, err := strconv.ParseFloat(m.StatDrainFailed, 64)
		if err == nil {
			o.StatDrainFailed.Reset()
			o.StatDrainFailed.WithLabelValues(m.Id, m.Host).Set(drainfailed)
		}

		diskiops, err := strconv.ParseFloat(m.StatDiskIops, 64)
		if err == nil {
			o.StatDiskIops.Reset()
			o.StatDiskIops.WithLabelValues(m.Id, m.Host).Set(diskiops)
		}

		diskbw, err := strconv.ParseFloat(m.StatDiskBw, 64)
		if err == nil {
			o.StatDiskBw.Reset()
			o.StatDiskBw.WithLabelValues(m.Id, m.Host).Set(diskbw)
		}

		// FS Active Status.

		active_status := 0
		switch stat := m.StatActive; stat {
		case "offline":
			active_status = 0
		case "online":
			active_status = 1
		default:
			active_status = 1
		}

		o.StatActive.Reset()
		o.StatActive.WithLabelValues(m.Id, m.Host).Set(float64(active_status))

		// Health

		health := 0
		if m.StatHealth == "OK" {
			health = 0
		} else {
			health = 1
		}
		o.StatHealth.Reset()
		o.StatHealth.WithLabelValues(m.Id, m.Host).Set(float64(health))
	}

	return nil

} // collectFSDF()

// Describe sends the descriptors of each FSCollector related metrics we have defined
func (o *FSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	//ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
func (o *FSCollector) Collect(ch chan<- prometheus.Metric) {

	if err := o.collectFSDF(); err != nil {
		log.Println("failed collecting fs metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}
}
