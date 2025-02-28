package eosclient

// Used to run eos commands.
// This code can be vastly improved.

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"os/exec"
	osuser "os/user"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"
	"unicode"

	"go.uber.org/zap"
)

var DEFAULT_TIMEOUT = 30 // Time-out in seconds for the EOS commands

type Options struct {
	// Location of the eos binary. Default is /usr/bin/eos.
	EosBinary string

	// Location of the xrdcopy binary. Default is /usr/bin/xrdcopy.
	XrdcopyBinary string

	// URL of the EOS MGM. Default is root://eos-test.org
	URL string

	// Location on the local fs where to store reads. Defaults to os.TempDir()
	CacheDirectory string

	// Enables logging of the commands executed. Defaults to false
	EnableLogging bool

	// Logger to use
	Logger *zap.Logger

	// Timeout number of seconds before timing out requests to EOS
	Timeout int
}

func (opt *Options) init() {
	if opt.EosBinary == "" {
		opt.EosBinary = "/usr/bin/eos"
	}

	if opt.XrdcopyBinary == "" {
		opt.XrdcopyBinary = "/usr/bin/xrdcopy"
	}

	if opt.URL == "" {
		opt.URL = "root://eos-example.org"
	}

	if opt.CacheDirectory == "" {
		opt.CacheDirectory = os.TempDir()
	}

	if opt.Logger == nil {
		l, _ := zap.NewProduction()
		opt.Logger = l
	}

	if opt.Timeout == 0 {
		opt.Timeout = DEFAULT_TIMEOUT
	}
}

// Client performs actions against a EOS management node (MGM).
// It requires the eos-client and xrootd-client packages installed to work.
type Client struct {
	opt *Options
}

type NodeInfo struct {
	Host                  string
	Port                  string
	Status                string
	CfgStatus             string
	Nofs                  string
	HeartBeatDelta        string
	SumStatStatfsFree     string
	SumStatStatfsUsed     string
	SumStatStatfsTotal    string
	SumStatStatFilesFree  string
	SumStatStatFilesUsed  string
	SumStatStatFilesTotal string
	SumStatRopen          string
	SumStatWopen          string
	CfgStatSysThreads     string
	CfgStatSysVsize       string
	CfgStatSysRss         string
	CfgStatSysSockets     string
	SumStatNetInratemib   string
	SumStatNetOutratemib  string
	EOSVersion            string
	XRootDVersion         string
	Kernel                string
	Geotag                string
}

type GroupInfo struct {
	Name                   string
	CfgStatus              string
	Nofs                   string
	AvgStatDiskLoad        string
	SigStatDiskLoad        string
	SumStatDiskReadratemb  string
	SumStatDiskWriteratemb string
	SumStatNetEthratemib   string
	SumStatNetInratemib    string
	SumStatNetOutratemib   string
	SumStatRopen           string
	SumStatWopen           string
	SumStatStatfsUsedbytes string
	SumStatStatfsFreebytes string
	SumStatStatfsCapacity  string
	SumStatUsedfiles       string
	SumStatStatfsFfree     string
	SumStatStatfsFiles     string
	DevStatStatfsFilled    string
	AvgStatStatfsFilled    string
	SigStatStatfsFilled    string
	CfgStatBalancing       string
	SumStatBalancerRunning string
	SumStatDrainerRunning  string
}

type FSInfo struct {
	Host                       string
	Port                       string
	Id                         string
	Uuid                       string
	Path                       string
	Schedgroup                 string
	StatBoot                   string
	Configstatus               string
	Headroom                   string
	StatErrc                   string
	StatErrmsg                 string
	StatDiskLoad               string
	StatDiskReadratemb         string
	StatDiskWriteratemb        string
	StatNetEthratemib          string
	StatNetInratemib           string
	StatNetOutratemib          string
	StatRopen                  string
	StatWopen                  string
	StatStatfsFreebytes        string
	StatStatfsUsedbytes        string
	StatStatfsCapacity         string
	StatUsedfiles              string
	StatStatfsFfree            string
	StatStatfsFused            string
	StatStatfsFiles            string
	Drainstatus                string
	StatDrainprogress          string
	StatDrainfiles             string
	StatDrainbytesleft         string
	StatDrainretry             string
	StatDrainFailed            string
	Graceperiod                string
	StatTimeleft               string
	StatActive                 string
	StatBalancerRunning        string
	StatDrainerRunning         string
	StatDiskIops               string
	StatDiskBw                 string
	StatGeotag                 string
	StatHealth                 string
	StatHealthRedundancyFactor string
	StatHealthDrivesFailed     string
	StatHealthDrivesTotal      string
	StatHealthIndicator        string
}

type NSInfo struct {
	Boot_file_time                             string
	Boot_status                                string
	Boot_time                                  string
	Cache_container_maxsize                    string
	Cache_container_occupancy                  string
	Cache_files_maxsize                        string
	Cache_files_occupancy                      string
	Fds_all                                    string
	Fusex_activeclients                        string
	Fusex_caps                                 string
	Fusex_clients                              string
	Fusex_lockedclients                        string
	Hanging_since                              string
	Latency_dirs                               string
	Latency_files                              string
	Latency_pending_updates                    string
	Latencypeak_eosviewmutex_1min              string
	Latencypeak_eosviewmutex_2min              string
	Latencypeak_eosviewmutex_5min              string
	Latencypeak_eosviewmutex_last              string
	Qclient_rtt_ms_min                         string
	Qclient_rtt_ms_avg                         string
	Qclient_rtt_ms_max                         string
	Qclient_rtt_ms_peak_1min                   string
	Qclient_rtt_ms_peak_2min                   string
	Qclient_rtt_ms_peak_5min                   string
	Memory_growth                              string
	Memory_resident                            string
	Memory_share                               string
	Memory_virtual                             string
	Stat_threads                               string
	Total_directories                          string
	Total_directories_changelog_avg_entry_size string
	Total_directories_changelog_size           string
	Total_files                                string
	Total_files_changelog_avg_entry_size       string
	Total_files_changelog_size                 string
	Uptime                                     string
}

type NSActivityInfo struct {
	User       string
	Gid        string
	Operation  string
	Sum        string
	Last_5s    string
	Last_60s   string
	Last_300s  string
	Last_3600s string
	Exec       string
	Sigma      string
	Exec99     string
	Max        string
}
type NSBatchInfo struct {
	User       string
	Operation  string
	Sum        string
	Last_5s    string
	Last_60s   string
	Last_300s  string
	Last_3600s string
	Level      string
}

type IOInfo struct {
	Measurement string
	Application string
	Total       string
	Last_60s    string
	Last_300s   string
	Last_3600s  string
	Last_86400s string
}

type Sys struct {
	Eos struct {
		Start   string `json:"start"`
		Version string `json:"version"`
	} `json:"eos"`
	Kernel  string     `json:"kernel"`
	Rss     *StringInt `json:"rss"`
	Sockets *StringInt `json:"sockets"`
	Threads int        `json:"threads"`
	Uptime  *StringInt `json:"uptime"`
	Vsize   int        `json:"vsize"`
	Xrootd  struct {
		Version string `json:"version"`
	} `json:"xrootd"`
}

type StringInt struct {
	value string
}

func (s *StringInt) UnmarshalJSON(data []byte) error {
	var v interface{}
	err := json.Unmarshal(data, &v)
	if err != nil {
		return err
	}

	switch t := v.(type) {
	case int:
		s.value = strconv.Itoa(t)
	case int32:
		s.value = strconv.Itoa(int(t))
	case int64:
		s.value = strconv.Itoa(int(t))
	case float32:
		s.value = strconv.FormatFloat(float64(t), 'g', 0, 64)
	case float64:
		s.value = strconv.FormatFloat(t, 'g', 0, 64)
	case string:
		s.value = t
	default:
		return errors.New("type not supported")
	}
	return nil
}

func (s *StringInt) MarshalJSON() ([]byte, error) {
	return []byte(s.value), nil
}

type Stat struct {
	Geotag string `json:"geotag"`
	Sys    Sys    `json:"sys"`
}

type NodeLSCfg struct {
	Stat Stat `json:"stat"`
}

type NodeLS struct {
	HostPort string     `json:"hostport"` // "hostname:port"
	Cfg      *NodeLSCfg `json:"cfg"`
}

type NodeLSResponse struct {
	ErrorMsg string    `json:"errormsg"`
	Result   []*NodeLS `json:"result"`
}

func New(opt *Options) (*Client, error) {
	opt.init()
	c := new(Client)
	c.opt = opt
	return c, nil
}

func getUnixUser(username string) (*osuser.User, error) {
	return osuser.Lookup(username)
}

// exec executes the command and returns the stdout, stderr and return code
func (c *Client) execute(cmd *exec.Cmd) (string, string, error) {
	cmd.Env = []string{
		"EOS_MGM_URL=" + c.opt.URL,
	}

	outBuf := &bytes.Buffer{}
	errBuf := &bytes.Buffer{}
	cmd.Stdout = outBuf
	cmd.Stderr = errBuf
	err := cmd.Run()
	if c.opt.EnableLogging {
		c.opt.Logger.Info("eosclient", zap.String("cmd", fmt.Sprintf("%+v", cmd)))
	}

	if exiterr, ok := err.(*exec.ExitError); ok {
		// The program has exited with an exit code != 0
		// This works on both Unix and Windows. Although package
		// syscall is generally platform dependent, WaitStatus is
		// defined for both Unix and Windows and in both cases has
		// an ExitStatus() method with the same signature.
		if status, ok := exiterr.Sys().(syscall.WaitStatus); ok {
			switch status.ExitStatus() {
			case 2:
				err = fmt.Errorf("error: FIXME") // api.NewError(api.StorageNotFoundErrorCode)
			}
		}
	}
	return outBuf.String(), errBuf.String(), err
}

func (c *Client) getTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	timeout := time.Duration(c.opt.Timeout) * time.Second
	return context.WithTimeout(ctx, time.Duration(timeout))
}

// List the nodes on the instance
func (c *Client) ListNode(ctx context.Context, username string) ([]*NodeInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "node", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseNodesInfo(stdout)
}

// List the scheduling groups on the instance
func (c *Client) ListGroup(ctx context.Context, username string) ([]*GroupInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "group", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseGroupsInfo(stdout)
}

// List the filesystems on the instance
func (c *Client) ListFS(ctx context.Context, username string) ([]*FSInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "fs", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseFSsInfo(stdout)
}

// List the activity of different users in the instance
func (c *Client) ListNS(ctx context.Context) ([]*NSInfo, []*NSActivityInfo, []*NSBatchInfo, error) {
	// eos ns stat, without -a will exclude batch users info (this adds to much latency in the instance where the exporter is deployed)
	stdout, _, err := c.execute(exec.CommandContext(ctx, "/usr/bin/eos", "ns", "stat", "-m"))
	if err != nil {
		return nil, nil, nil, err
	}

	stdo, _, err2 := c.execute(exec.CommandContext(ctx, "/usr/bin/eos", "who", "-a", "-m"))
	if err2 != nil {
		return nil, nil, nil, err2
	}

	return c.parseNSsInfo(stdout, stdo, ctx)
}

// List the IO info in the instance
func (c *Client) ListIOInfo(ctx context.Context) ([]*IOInfo, error) {

	ctx, _ = c.getTimeout(ctx)

	stdout1, _, err := c.execute(exec.CommandContext(ctx, "/usr/bin/eos", "io", "stat", "-m"))
	if err != nil {
		return nil, err
	}

	return c.parseIOInfosInfo(stdout1, ctx)
}

// List the IO info in the instance
func (c *Client) ListIOAppInfo(ctx context.Context) ([]*IOInfo, error) {

	ctx, cancel := c.getTimeout(ctx)
	defer cancel()

	stdout2, _, err := c.execute(exec.CommandContext(ctx, "/usr/bin/eos", "io", "stat", "-m", "-x"))
	if err != nil {
		return nil, err
	}

	return c.parseIOAppInfosInfo(stdout2, ctx)
}

func getHostname(hostport string) (string, string, bool) {
	return strings.Cut(hostport, ":")
}

// Convert a monitoring format line into a map
func (c *Client) getMap(line string) map[string]string {
	lastQuote := rune(0)
	f := func(c rune) bool {
		switch {
		case c == lastQuote:
			lastQuote = rune(0)
			return false
		case lastQuote != rune(0):
			return false
		case unicode.In(c, unicode.Quotation_Mark):
			lastQuote = c
			return false
		default:
			return unicode.IsSpace(c)

		}
	}

	// splitting string by space but considering quoted section
	items := strings.FieldsFunc(line, f)

	// create and fill the map
	m := make(map[string]string)
	for _, item := range items {
		k, v, found := strings.Cut(item, "=")
		if found {
			// simply drop the '???' values, rely on defaults instead
			if v != "???" {
				m[k] = v
			}
		} else {
			c.opt.Logger.Info("wrong format, expect key=value", zap.String("item", item))
		}
	}
	return m

}

// Gathers information of all nodes
func (c *Client) parseNodesInfo(raw string) ([]*NodeInfo, error) {
	fstinfos := []*NodeInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		node, err := c.parseNodeInfo(rl)

		if err != nil {
			c.opt.Logger.Info("bad nodeinfo", zap.Error(err))
			continue
		}
		fstinfos = append(fstinfos, node)
	}
	return fstinfos, nil
}

// Gathers information of one single node
func (c *Client) parseNodeInfo(line string) (*NodeInfo, error) {
	//kv := make(map[string]string)
	kv := c.getMap(line)
	host, port, foundcolon := getHostname(kv["hostport"])
	if !foundcolon {
		return nil, fmt.Errorf("bad hostport: %s", kv["hostport"])
	}
	fst := &NodeInfo{
		Host:                  host,
		Port:                  port,
		Status:                kv["status"],
		CfgStatus:             kv["cfg.status"],
		Nofs:                  kv["nofs"],
		HeartBeatDelta:        kv["heartbeatdelta"],
		SumStatStatfsFree:     kv["sum.stat.statfs.freebytes"],
		SumStatStatfsUsed:     kv["sum.stat.statfs.usedbytes"],
		SumStatStatfsTotal:    kv["sum.stat.statfs.capacity"],
		SumStatStatFilesFree:  kv["sum.stat.statfs.ffree"],
		SumStatStatFilesUsed:  kv["sum.stat.usedfiles"],
		SumStatStatFilesTotal: kv["sum.stat.statfs.files"],
		SumStatRopen:          kv["sum.stat.ropen"],
		SumStatWopen:          kv["sum.stat.wopen"],
		CfgStatSysThreads:     kv["cfg.stat.sys.threads"],
		CfgStatSysVsize:       kv["cfg.stat.sys.vsize"],
		CfgStatSysRss:         kv["cfg.stat.sys.rss"],
		CfgStatSysSockets:     kv["cfg.stat.sys.sockets"],
		SumStatNetInratemib:   kv["sum.stat.net.inratemib"],
		SumStatNetOutratemib:  kv["sum.stat.net.outratemib"],
		EOSVersion:            kv["cfg.stat.sys.eos.version"],
		XRootDVersion:         kv["cfg.stat.sys.xrootd.version"],
		Kernel:                kv["cfg.stat.sys.kernel"],
		Geotag:                kv["cfg.stat.geotag"],
	}
	return fst, nil
}

// Gathers information of all groups
func (c *Client) parseGroupsInfo(raw string) ([]*GroupInfo, error) {
	groupinfos := []*GroupInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		group, err := c.parseGroupInfo(rl)

		if err != nil {
			return nil, err
		}
		groupinfos = append(groupinfos, group)
	}
	return groupinfos, nil
}

// Gathers information of one single group
func (c *Client) parseGroupInfo(line string) (*GroupInfo, error) {
	//kv := make(map[string]string)
	kv := c.getMap(line)
	group := &GroupInfo{
		kv["name"],
		kv["cfg.status"],
		kv["nofs"],
		kv["avg.stat.disk.load"],
		kv["sig.stat.disk.load"],
		kv["sum.stat.disk.readratemb"],
		kv["sum.stat.disk.writeratemb"],
		kv["sum.stat.net.ethratemib"],
		kv["sum.stat.net.inratemib"],
		kv["sum.stat.net.outratemib"],
		kv["sum.stat.ropen"],
		kv["sum.stat.wopen"],
		kv["sum.stat.statfs.usedbytes"],
		kv["sum.stat.statfs.freebytes"],
		kv["sum.stat.statfs.capacity"],
		kv["sum.stat.usedfiles"],
		kv["sum.stat.statfs.ffree"],
		kv["sum.stat.statfs.files"],
		kv["dev.stat.statfs.filled"],
		kv["avg.stat.statfs.filled"],
		kv["sig.stat.statfs.filled"],
		kv["cfg.stat.balancing"],
		kv["sum.stat.balancer.running"],
		kv["sum.stat.drainer.running"],
	}
	return group, nil
}

// Gathers information of all filesystems
func (c *Client) parseFSsInfo(raw string) ([]*FSInfo, error) {
	fsinfos := []*FSInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		fs, err := c.parseFSInfo(rl)

		if err != nil {
			return nil, err
		}
		fsinfos = append(fsinfos, fs)
	}
	return fsinfos, nil
}

// Gathers information of one single filesystem
func (c *Client) parseFSInfo(line string) (*FSInfo, error) {
	//kv := make(map[string]string)
	kv := c.getMap(line)
	fs := &FSInfo{
		kv["host"],
		kv["port"],
		kv["id"],
		kv["uuid"],
		kv["path"],
		kv["schedgroup"],
		kv["stat.boot"],
		kv["configstatus"],
		kv["headroom"],
		kv["stat.errc"],
		kv["stat.errmsg"],
		kv["stat.disk.load"],
		kv["stat.disk.readratemb"],
		kv["stat.disk.writeratemb"],
		kv["stat.net.ethratemib"],
		kv["stat.net.inratemib"],
		kv["stat.net.outratemib"],
		kv["stat.ropen"],
		kv["stat.wopen"],
		kv["stat.statfs.freebytes"],
		kv["stat.statfs.usedbytes"],
		kv["stat.statfs.capacity"],
		kv["stat.usedfiles"],
		kv["stat.statfs.ffree"],
		kv["stat.statfs.fused"],
		kv["stat.statfs.files"],
		kv["drainstatus"],
		kv["stat.drainprogress"],
		kv["stat.drainfiles"],
		kv["stat.drainbytesleft"],
		kv["stat.drainretry"],
		kv["stat.drain.failed"],
		kv["graceperiod"],
		kv["stat.timeleft"],
		kv["stat.active"],
		kv["stat.balancer.running"],
		kv["stat.drainer.running"],
		kv["stat.disk.iops"],
		kv["stat.disk.bw"],
		kv["stat.geotag"],
		kv["stat.health"],
		kv["stat.health.redundancy_factor"],
		kv["stat.health.drives_failed"],
		kv["stat.health.drives_total"],
		kv["stat.health.indicator"],
	}
	return fs, nil
}

// Checks if uid is made only of letters.
func UidLetter(s string) bool {
	for _, r := range s {
		if !unicode.IsLetter(r) {
			return false
		}
	}
	return true
}

func onlyUsers(a string, list []string) bool {
	for _, b := range list {
		if b == a {
			return false
		}
	}
	return true
}

func isInMap(a string, m map[string]int) bool {
	for k := range m {
		if k == a {
			return true
		}
	}
	return false
}

// Gathers information of the namespace
func (c *Client) parseNSsInfo(raw string, raw_batch string, ctx context.Context) ([]*NSInfo, []*NSActivityInfo, []*NSBatchInfo, error) {
	var kv map[string]string
	var kvb map[string]string
	var nsinfo *NSInfo
	var nsactinfo *NSActivityInfo
	var nsbatchinfo *NSBatchInfo
	nsinfos := []*NSInfo{}
	nsactinfos := []*NSActivityInfo{}
	nsbatchinfos := []*NSBatchInfo{}
	rawLines := strings.Split(raw, "\n")
	rawBatchLines := strings.Split(raw_batch, "\n")
	batchUsers := make(map[string]int)
	batchMetrics := make(map[string]bool)
	excl_uids := []string{"root", "nobody", "daemon", "wwweos", "all"}
	for _, rlb := range rawBatchLines {
		if rlb == "" {
			continue
		}
		kvb = c.getMap(rlb)
		// Detect batch users 'eos who showing @b7 string'
		if strings.Contains(kvb["client"], "@b7") {
			// create a uid unique list of batch users
			if isInMap(kvb["uid"], batchUsers) {
				batchUsers[kvb["uid"]] += 1
			} else {
				batchUsers[kvb["uid"]] = 1
			}
		}
	}
	// First iteration to find out Stalled operations
	for _, rl := range rawLines {
		if strings.Contains(rl, "Stall::") {
			kv = c.getMap(rl)
		}
		// Get all letter uids, and exclude (root|daemon|nobody|wwweos)
		if UidLetter(kv["uid"]) {
			has, excl := false, false
			periods := []string{ /*"5s", */ "60s", "300s", "3600s"}
			// Check that user is not in the excluded list but is in the batch users' list
			if onlyUsers(kv["uid"], excl_uids) && isInMap(kv["uid"], batchUsers) {
				/*// Values for testing
				if batchUsers[kv["uid"]] >= 2 {*/
				if batchUsers[kv["uid"]] >= 500 {
					// If more than one value in periods is zero, not trigger metric.
					for _, k := range periods {
						flVar, err := strconv.ParseFloat(kv[k], 32)
						if int(flVar) == 0 && err == nil {
							if has {
								excl = true
							} else {
								has = true
							}
						}
						if err != nil {
							fmt.Printf("%s -> in period %s of op: %s of user %s\n", err, k, kv["cmd"], kv["uid"])
						}
					}
					//if excl { // For testing purposes
					if !excl {
						batchMetrics[kv["uid"]+"-"+strings.Replace(kv["cmd"], "Stall::", "", -1)] = true
					}
				}
			}
		}
	}
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		kv = c.getMap(rl)
		// Only expose global data, without breakdown of users
		if kv["uid"] == "all" && kv["gid"] == "all" {
			// Separate activity info from namespace statistics info
			if _, ok := kv["cmd"]; ok {
				if kv["5s"] == "0.00" && kv["60s"] == "0.00" && kv["300s"] == "0.00" && kv["3600s"] == "0.00" {
				} else {
					nsactinfo = &NSActivityInfo{
						kv["uid"],
						kv["gid"],
						kv["cmd"],
						kv["total"],
						kv["5s"],
						kv["60s"],
						kv["300s"],
						kv["3600s"],
						kv["exec"],
						kv["execsig"],
						kv["exec99"],
						kv["execmax"],
					}
				}
			} else {
				if len(kv) <= 3 {
					for k := range kv {
						if k != "uid" && k != "gid" {
							if _, err := strconv.ParseFloat(kv[k], 64); err != nil {
								fmt.Sprintf("Value of '%s': '%s' is not floatable", k, kv[k])
							}
							nsinfo = &NSInfo{
								kv["ns.boot.file.time"],
								kv["ns.boot.status"],
								kv["ns.boot.time"],
								kv["ns.cache.containers.maxsize"],
								kv["ns.cache.containers.occupancy"],
								kv["ns.cache.files.maxsize"],
								kv["ns.cache.files.occupancy"],
								kv["ns.fds.all"],
								kv["ns.fusex.activeclients"],
								kv["ns.fusex.caps"],
								kv["ns.fusex.clients"],
								kv["ns.fusex.lockedclients"],
								kv["ns.hanging.since"],
								kv["ns.latency.dirs"],
								kv["ns.latency.files"],
								kv["ns.latency.pending.updates"],
								kv["ns.latencypeak.eosviewmutex.1min"],
								kv["ns.latencypeak.eosviewmutex.2min"],
								kv["ns.latencypeak.eosviewmutex.5min"],
								kv["ns.latencypeak.eosviewmutex.last"],
								kv["ns.qclient.rtt_ms.min"],
								kv["ns.qclient.rtt_ms.avg"],
								kv["ns.qclient.rtt_ms.max"],
								kv["ns.qclient.rtt_ms_peak.1min"],
								kv["ns.qclient.rtt_ms_peak.2min"],
								kv["ns.qclient.rtt_ms_peak.5min"],
								kv["ns.memory.growth"],
								kv["ns.memory.resident"],
								kv["ns.memory.share"],
								kv["ns.memory.virtual"],
								kv["ns.stat.threads"],
								kv["ns.total.directories"],
								kv["ns.total.directories.changelog.avg_entry_size"],
								kv["ns.total.directories.changelog.size"],
								kv["ns.total.files"],
								kv["ns.total.files.changelog.avg_entry_size"],
								kv["ns.total.files.changelog.size"],
								kv["ns.uptime"],
							}
						}
					}
				}
			}
		}
		// Check that user has stall operation and is actually that operation to be exposed, plus is legitimate user
		if UidLetter(kv["uid"]) && onlyUsers(kv["uid"], excl_uids) && batchMetrics[kv["uid"]+"-"+kv["cmd"]] {
			var eos_instance string = "homecanary"
			level := 0
			ctx, cancel := c.getTimeout(ctx)
			defer cancel()

			stdo, _, err := c.execute(exec.CommandContext(ctx, "eos", "version"))
			if err != nil {
				fmt.Println("Couldn't get the EOS instance")
			}
			eos_ins_out := strings.Split(string(stdo), "\n")
			for _, line := range eos_ins_out {
				// Get eos instance name to be used for getting the latency values.
				if strings.HasPrefix(line, "EOS_INSTANCE") {
					eos_instance = strings.Split(line, "=eos")[1]
				}
			}
			cmd := exec.Command("python2", "-c", "import sys;sys.path.append('/usr/local/sbin/');import eos_graphite as eg;print(eg.get_ns_latency('"+eos_instance+"',eg.PREFIX))")
			stdo2, err := cmd.CombinedOutput()
			if err != nil {
				fmt.Println("Not able to get ns latency.")
			}
			parse_latency := strings.Split(string(stdo2), ", (")
			whoami_lat, err := strconv.ParseFloat(strings.TrimRight(strings.Split(parse_latency[1], ", ")[1], "))"), 32)
			touch_lat, err := strconv.ParseFloat(strings.TrimRight(strings.Split(parse_latency[3], ", ")[1], "))"), 32)
			ls_lat, err := strconv.ParseFloat(strings.TrimRight(strings.Split(parse_latency[9], ", ")[1], "))"), 32)

			stdout, _, err := c.execute(exec.CommandContext(ctx, "id", kv["uid"]))
			if err != nil {
				fmt.Printf("Couldn't get the uid of %s\n", kv["uid"])
			} else {
				kv["uid"] = strings.Split(strings.TrimLeft(stdout, "uid="), "(")[0]
			}
			/*// For testing
			fmt.Printf("Uid: %s: cmd: %s, total: %s\n", kv["uid"], kv["cmd"], kv["total"])
			fmt.Printf("Whoami Latency: %f\nTouch Latency: %f\nRm Latency: %f\nMkdir Latency: %f\nLs Latency: %f\nRmdir Latency: %f\n", whoami_lat, touch_lat, rm_lat, mkdir_lat, ls_lat, rmdir_lat)*/
			// Define threshold for defining impact levels
			thresholds := []float64{0.05, 0.5, 2} // 50 ms , 500ms and 2 sec
			if whoami_lat >= thresholds[2] && ls_lat >= thresholds[2] && touch_lat >= thresholds[2] {
				level = 3
			} else if whoami_lat >= thresholds[1] && ls_lat >= thresholds[1] && touch_lat >= thresholds[1] {
				level = 2
			} else if whoami_lat >= thresholds[0] && ls_lat >= thresholds[0] && touch_lat >= thresholds[0] {
				level = 1
			} else {
				level = 0
			}
			nsbatchinfo = &NSBatchInfo{
				kv["uid"],
				kv["cmd"],
				kv["total"],
				kv["5s"],
				kv["60s"],
				kv["300s"],
				kv["3600s"],
				strconv.Itoa(level),
			}
		}
		if nsinfo != nil {
			nsinfos = append(nsinfos, nsinfo)
		}
		if nsactinfo != nil {
			nsactinfos = append(nsactinfos, nsactinfo)
		}
		if nsbatchinfo != nil {
			nsbatchinfos = append(nsbatchinfos, nsbatchinfo)
		}
	}
	return nsinfos, nsactinfos, nsbatchinfos, nil
}

// Gathers information of IO stats
func (c *Client) parseIOInfosInfo(raw1 string, ctx context.Context) ([]*IOInfo, error) {
	ioinfos := []*IOInfo{}
	rawLines := strings.Split(raw1, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		ioinfo, err := c.parseIOInfo(rl)

		if err != nil {
			return nil, err
		}
		ioinfos = append(ioinfos, ioinfo)
	}

	return ioinfos, nil
}

// Gathers information of IO stats
func (c *Client) parseIOAppInfosInfo(raw2 string, ctx context.Context) ([]*IOInfo, error) {
	ioinfos := []*IOInfo{}

	rawLines := strings.Split(raw2, "\n")
	for _, rlx := range rawLines {
		if rlx == "" {
			continue
		}
		ioinfo, err := c.parseAppIOInfo(rlx)

		if err != nil {
			return nil, err
		}
		ioinfos = append(ioinfos, ioinfo)
	}

	return ioinfos, nil
}

// Gathers information of one single IO stat
func (c *Client) parseIOInfo(line string) (*IOInfo, error) {
	kv := c.getMap(line)
	ioinfo := &IOInfo{
		Measurement: kv["measurement"],
		Application: "NA",
		Total:       kv["total"],
		Last_60s:    kv["60s"],
		Last_300s:   kv["300s"],
		Last_3600s:  kv["3600s"],
		Last_86400s: kv["86400s"],
	}
	return ioinfo, nil
}

// Gathers information of one IO stat classified by app
func (c *Client) parseAppIOInfo(line string) (*IOInfo, error) {
	kv := c.getMap(line)
	ioinfo := &IOInfo{
		Measurement: kv["measurement"],
		Application: kv["application"],
		Total:       kv["total"],
		Last_60s:    kv["60s"],
		Last_300s:   kv["300s"],
		Last_3600s:  kv["3600s"],
		Last_86400s: kv["86400s"],
	}
	return ioinfo, nil
}

// ----------------------------------------//
// RECYCLE BIN INFORMATION 			       //
// ----------------------------------------//

// Data struct //
type RecycleInfo struct {
	UsedBytes string
	MaxBytes  string
	Lifetime  string
	Ratio     string
}

// Launch recycle command //
func (c *Client) Recycle(ctx context.Context, username string) ([]*RecycleInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "recycle", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseRecycleInfo(stdout)
}

// Parse information from recycle bin //
func (c *Client) parseRecycleInfo(raw string) ([]*RecycleInfo, error) {
	recycleInfo := []*RecycleInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		recycle, err := c.parseRecycleLineInfo(rl)
		if err != nil {
			return nil, err
		}
		recycleInfo = append(recycleInfo, recycle)
	}
	return recycleInfo, nil
}

func (c *Client) parseRecycleLineInfo(line string) (*RecycleInfo, error) {
	kv := c.getMap(line)
	rb := &RecycleInfo{
		kv["usedbytes"],
		kv["maxbytes"],
		kv["lifetime"],
		kv["ratio"],
	}
	return rb, nil
}

// Data struct //
type QuotaInfo struct {
	Uid              string
	Gid              string
	Space            string
	UsedBytes        int64
	MaxBytes         int64
	UsedLogicalBytes int64
	MaxLogicalBytes  int64
	UsedFiles        int64
	MaxFiles         int64
}

// Launch who command //
func (c *Client) Quotas(ctx context.Context, username string) ([]*QuotaInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}
	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "quota", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseQuotaInfo(stdout)
}

// Parse information from recycle bin //
func (c *Client) parseQuotaInfo(raw string) ([]*QuotaInfo, error) {
	whoInfo := []*QuotaInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}

		kv := c.getMap(rl)
		uid, okuid := kv["uid"]
		gid, okgid := kv["gid"]
		if !okuid && !okgid {
			continue
		}

		usedBytes, _ := strconv.ParseInt(kv["usedbytes"], 10, 64)
		maxBytes, _ := strconv.ParseInt(kv["maxbytes"], 10, 64)
		usedLogicalBytes, _ := strconv.ParseInt(kv["usedlogicalbytes"], 10, 64)
		maxLogicalBytes, _ := strconv.ParseInt(kv["maxlogicalbytes"], 10, 64)
		usedFiles, _ := strconv.ParseInt(kv["usedfiles"], 10, 64)
		maxFiles, _ := strconv.ParseInt(kv["maxfiles"], 10, 64)

		who := &QuotaInfo{
			Uid:              uid,
			Gid:              gid,
			Space:            kv["space"],
			UsedBytes:        usedBytes,
			MaxBytes:         maxBytes,
			UsedLogicalBytes: usedLogicalBytes,
			MaxLogicalBytes:  maxLogicalBytes,
			UsedFiles:        usedFiles,
			MaxFiles:         maxFiles,
		}

		whoInfo = append(whoInfo, who)
	}
	return whoInfo, nil
}

// ----------------------------------------//
// EOS WHO    INFORMATION 			       //
// ----------------------------------------//

// eos who -a -m provides 3 clusters of information
// a) Aggregation of number of sessions by protocol, examples:
// auth=gsi nsessions=2
// auth=https nsessions=3093
// b) Aggregation by uid
// uid=982 nsessions=2
// uid=983 nsessions=2
// c) Client info
// client=yyyy@xxxx.cern.ch uid=yyyy auth=https idle=66 gateway="xxxx.cern.ch" app=http
// Because a) and b) can be derived from c), we only report c) in the metric
// Aggregation on fields from c) can be done in the monitoring system

// Data struct //
type WhoInfo struct {
	Uid        string
	Auth       string
	Gateway    string
	App        string
	Serialized string // used for uniqueness

}

func (w *WhoInfo) serialize() {
	w.Serialized = fmt.Sprintf("%s:::%s:::%s:::%s", w.Uid, w.Auth, w.Gateway, w.App)
}

func (w *WhoInfo) String() string {
	return w.Serialized
}

// Launch who command //
func (c *Client) Who(ctx context.Context, username string) ([]*WhoInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}
	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "who", "-a", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseWhoInfo(stdout)
}

// Parse information from recycle bin //
func (c *Client) parseWhoInfo(raw string) ([]*WhoInfo, error) {
	whoInfo := []*WhoInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}

		kv := c.getMap(rl)
		if _, ok := kv["client"]; !ok {
			continue
		}

		who := &WhoInfo{
			Uid:     kv["uid"],
			Gateway: strings.Trim(kv["gateway"], "\""), // clean double quotes
			Auth:    kv["auth"],
			App:     kv["app"],
		}
		who.serialize()

		whoInfo = append(whoInfo, who)
	}
	return whoInfo, nil
}

// ----------------------------------------//
// EOS SPACE    INFORMATION 			    //
// ----------------------------------------//

// struct definition
type SpaceInfo struct {
	Type                                 string
	Name                                 string
	CfgGroupSize                         string
	CfgGroupMod                          string
	Nofs                                 string
	AvgStatDiskLoad                      string
	SigStatDiskLoad                      string
	SumStatDiskReadratemb                string
	SumStatDiskWriteratemb               string
	SumStatNetEthratemib                 string
	SumStatNetInratemib                  string
	SumStatNetOutratemib                 string
	SumStatRopen                         string
	SumStatWopen                         string
	SumStatStatfsUsedbytes               string
	SumStatStatfsFreebytes               string
	SumStatStatfsCapacity                string
	SumStatUsedfiles                     string
	SumStatStatfsFfiles                  string
	SumStatStatfsFiles                   string
	SumStatStatfsCapacityConfigstatusRw  string
	SumNofsConfigstatusRw                string
	CfgQuota                             string
	CfgNominalsize                       string
	CfgBalancer                          string
	CfgBalancerThreshold                 string
	SumStatBalancerRunning               string
	SumStatDrainerRunning                string
	SumStatDiskIopsConfigstatusRw        string
	SumStatDiskBwConfigstatusRw          string
	SumStatStatfsFreebytesConfigstatusRw string
}

// List the spaces on the instance
func (c *Client) ListSpace(ctx context.Context, username string) ([]*SpaceInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "space", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseSpacesInfo(stdout)
}

// Gathers the information of all spaces.
func (c *Client) parseSpacesInfo(raw string) ([]*SpaceInfo, error) {
	spaceinfos := []*SpaceInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		space, err := c.parseSpaceInfo(rl)

		if err != nil {
			return nil, err
		}
		spaceinfos = append(spaceinfos, space)
	}
	return spaceinfos, nil
}

// Gathers information of one single space
func (c *Client) parseSpaceInfo(line string) (*SpaceInfo, error) {
	//kv := make(map[string]string)
	kv := c.getMap(line)
	space := &SpaceInfo{
		kv["type"],
		kv["name"],
		kv["cfg.groupsize"],
		kv["cfg.groupmod"],
		kv["nofs"],
		kv["avg.stat.disk.load"],
		kv["sig.stat.disk.load"],
		kv["sum.stat.disk.readratemb"],
		kv["sum.stat.disk.writeratemb"],
		kv["sum.stat.net.ethratemib"],
		kv["sum.stat.net.inratemib"],
		kv["sum.stat.net.outratemib"],
		kv["sum.stat.ropen"],
		kv["sum.stat.wopen"],
		kv["sum.stat.statfs.usedbytes"],
		kv["sum.stat.statfs.freebytes"],
		kv["sum.stat.statfs.capacity"],
		kv["sum.stat.usedfiles"],
		kv["sum.stat.statfs.ffiles"],
		kv["sum.stat.statfs.files"],
		kv["sum.stat.statfs.capacity?configstatus@rw"],
		kv["sum.<n>?configstatus@rw"],
		kv["cfg.quota"],
		kv["cfg.nominalsize"],
		kv["cfg.balancer"],
		kv["cfg.balancer.threshold"],
		kv["sum.stat.balancer.running"],
		kv["sum.stat.drainer.running"],
		kv["sum.stat.disk.iops?configstatus@rw"],
		kv["sum.stat.disk.bw?configstatus@rw"],
		kv["sum.stat.statfs.freebytes?configstatus@rw"],
	}
	return space, nil
}

// ----------------------------------------//
// EOS FSCK    INFORMATION 			       //
// ----------------------------------------//
// Gathers metrics from `eos fsck stat`
// Currently not in monitoring format
// `eos fsck report` is more detailed, but can be expensive.

// Data struct //
type FsckInfo struct {
	Tag   string
	Count string
}

// EOS command call and data extraction
func (c *Client) FsckReport(ctx context.Context, username string) ([]*FsckInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	//cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "fsck", "report", "-a")
	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "fsck", "stat")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseFsckInfo(stdout)
}

// Parse information from fsck report //
func (c *Client) parseFsckInfo(raw string) ([]*FsckInfo, error) {
	fsckInfo := []*FsckInfo{}
	rawLines := strings.Split(raw, "\n")
	var re = regexp.MustCompile(`d_cx_diff|d_mem_sz_diff|m_cx_diff|m_mem_sz_diff|orphans_n|rep_diff_n|rep_missing_n|unreg_n|blockxs_err|stripe_err`)
	for _, rl := range rawLines {
		if !strings.Contains(rl, "Info") && re.MatchString(rl) {
			fsck, err := c.parseFsckLineInfo(rl)
			if err != nil {
				return nil, err
			}
			fsckInfo = append(fsckInfo, fsck)
		} else {
			continue
		}

	}
	return fsckInfo, nil
}

func (c *Client) parseFsckLineInfo(line string) (*FsckInfo, error) {
	fields := strings.Fields(line)
	rb := &FsckInfo{
		Tag:   fields[3],
		Count: fields[5],
	}
	return rb, nil
}

// ----------------------------------------//
// EOS FUSEX MOUNTS INFORMATION 		   //
// ----------------------------------------//
// eos fusex ls -m

// struct definition
type FusexInfo struct {
	Host    string
	Version string
}

// List the fusexs on the instance
func (c *Client) ListFusex(ctx context.Context, username string) ([]*FusexInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "fusex", "ls", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseFusexsInfo(stdout)
}

// Gathers the information of all fusexs.
func (c *Client) parseFusexsInfo(raw string) ([]*FusexInfo, error) {
	fusexinfos := []*FusexInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" {
			continue
		}
		fusex, err := c.parseFusexInfo(rl)

		if err != nil {
			return nil, err
		}
		fusexinfos = append(fusexinfos, fusex)
	}
	return fusexinfos, nil
}

// Gathers information of one single fusex
func (c *Client) parseFusexInfo(line string) (*FusexInfo, error) {
	kv := c.getMap(line)
	fusex := &FusexInfo{
		kv["host"],
		kv["version"],
	}
	return fusex, nil
}

// ----------------------------------------//
// EOS INSPECTOR INFORMATION 		       //
// ----------------------------------------//

//LAYOUT INFO
// eos inspector -m | grep layout
// key=last layout=00000000 type=plain nominal_stripes=1 checksum=none blockchecksum=none blocksize=4k locations=0 nolocation=51051 physicalsize=78670882614 repdelta:-1=51051 unlinkedlocations=0 volume=78670882614 zerosize=51016
// eos_inspector_layout_volume{layout="00000000", type="plain", nominal_stripes="1", blocksize="4k"} 78670882614

//ACCESS TIME VOLUME
// eos inspector -m | grep accesstime::volume
// key=last tag=accesstime::volume bin=86400 value=3273370937835520
// eos_inspector_accesstime_volume{bin="86400"} 3273370937835520

// ACCESS TIME FILES
// eos inspector -m | grep accesstime::files
// key=last tag=accesstime::files bin=86400 value=5455901
// eos_inspector_accesstime_files{bin="86400"} 5455901

// BIRTH TIME VOLUME
// eos inspector -m | grep birthtime::volume
// key=last tag=birthtime::volume bin=0 value=916068146306018
// eos_inspector_birthtime_volume{bin="0"} 916068146306018

// BIRTH TIME FILES
// eos inspector -m | grep birthtime::files
// key=last tag=birthtime::files bin=86400 value=4670044
// eos_inspector_accesstime_files{bin="86400"} 4670044

// GROUP COST DISK
// eos inspector -m | grep group::cost::disk
// key=last tag=group::cost::disk groupname=root gid=0 cost=263.975993 price=20.000000 tbyears=13.198800
// eos_inspector_group_cost{groupname="root", gid=0, price=20.000000, ybin=0"} 263.975993

// GROUP TBYEARS DISK
// eos inspector -m | grep group::cost::disk
// key=last tag=group::cost::disk groupname=root gid=0 cost=263.975993 price=20.000000 tbyears=13.198800
// eos_inspector_group_cost{groupname="root", gid=0, price=20.000000, ybin=0"} 13.198800

// struct definition
type InspectorLayoutInfo struct {
	Layout         string
	Type           string
	NominalStripes string
	BlockSize      string
	Volume         string
}

// struct definition
type InspectorAccessTimeVolumeInfo struct {
	Bin    string
	Volume string
}

// struct definition
type InspectorAccessTimeFilesInfo struct {
	Bin   string
	Files string
}

// struct definition
type InspectorBirthTimeVolumeInfo struct {
	Bin    string
	Volume string
}

// struct definition
type InspectorBirthTimeFilesInfo struct {
	Bin   string
	Files string
}

// struct definition
type InspectorGroupCostDiskInfo struct {
	Groupname string
	Price     string
	Cost      string
}

// struct definition
type InspectorGroupCostDiskTBYearsInfo struct {
	Groupname string
	TBYears   string
}

func secondsToHumanReadable(secondsStr string) string {
	seconds, err := strconv.Atoi(secondsStr)
	if err != nil {
		// Handle the error (e.g., invalid input)
		return "Invalid input"
	}

	if seconds == 0 {
		return "0D"
	}

	duration := time.Second * time.Duration(seconds)
	days := int(duration.Hours() / 24)
	weeks := days / 7
	months := (days % 365) / 30
	years := days / 365

	// Round to the nearest year, month, week, or day
	if (days%365)*2 > 365 {
		years++
	} else if (days%30)*2 > 30 {
		months++
	} else if days%7 > 3 {
		weeks++
	}

	var result string

	if years > 0 {
		result += fmt.Sprintf("%dY", years)
	} else if months > 0 {
		result += fmt.Sprintf("%dM", months)
	} else if weeks > 0 {
		result += fmt.Sprintf("%dW", weeks)
	} else {
		result += fmt.Sprintf("%dD", days)
	}

	return result
}

// List Inspector Layout
func (c *Client) ListInspectorLayout(ctx context.Context, username string) ([]*InspectorLayoutInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorLayoutsInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorLayoutsInfo(raw string) ([]*InspectorLayoutInfo, error) {
	inspectorLayoutInfos := []*InspectorLayoutInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "layout") {
			continue
		}
		inspectorLayout, err := c.parseInspectorLayoutLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorLayoutInfos = append(inspectorLayoutInfos, inspectorLayout)
	}
	return inspectorLayoutInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorLayoutLine(line string) (*InspectorLayoutInfo, error) {
	kv := c.getMap(line)
	layoutInfo := &InspectorLayoutInfo{
		kv["layout"],
		kv["type"],
		kv["nominal_stripes"],
		kv["blocksize"],
		kv["volume"],
	}
	return layoutInfo, nil
}

// List Inspector AccessTime Volume
func (c *Client) ListInspectorAccessTimeVolume(ctx context.Context, username string) ([]*InspectorAccessTimeVolumeInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorAccessTimeVolumeInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorAccessTimeVolumeInfo(raw string) ([]*InspectorAccessTimeVolumeInfo, error) {
	inspectorAccessTimeVolumeInfos := []*InspectorAccessTimeVolumeInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "accesstime::volume") {
			continue
		}
		inspectorAccessTimeVolume, err := c.parseInspectorAccessTimeVolumeLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorAccessTimeVolumeInfos = append(inspectorAccessTimeVolumeInfos, inspectorAccessTimeVolume)
	}
	return inspectorAccessTimeVolumeInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorAccessTimeVolumeLine(line string) (*InspectorAccessTimeVolumeInfo, error) {
	kv := c.getMap(line)
	accessTimeVolumeInfo := &InspectorAccessTimeVolumeInfo{
		secondsToHumanReadable(kv["bin"]),
		kv["value"],
	}
	return accessTimeVolumeInfo, nil
}

// List Inspector Access Time Files
func (c *Client) ListInspectorAccessTimeFiles(ctx context.Context, username string) ([]*InspectorAccessTimeFilesInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorAccessTimeFilesInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorAccessTimeFilesInfo(raw string) ([]*InspectorAccessTimeFilesInfo, error) {
	inspectorAccessTimeFilesInfos := []*InspectorAccessTimeFilesInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "accesstime::files") {
			continue
		}
		inspectorAccessTimeFiles, err := c.parseInspectorAccessTimeFilesLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorAccessTimeFilesInfos = append(inspectorAccessTimeFilesInfos, inspectorAccessTimeFiles)
	}
	return inspectorAccessTimeFilesInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorAccessTimeFilesLine(line string) (*InspectorAccessTimeFilesInfo, error) {
	kv := c.getMap(line)
	accessTimeFilesInfo := &InspectorAccessTimeFilesInfo{
		secondsToHumanReadable(kv["bin"]),
		kv["value"],
	}
	return accessTimeFilesInfo, nil
}

// BIRTHTIME METRICS
// List Inspector BirthTime Volume
func (c *Client) ListInspectorBirthTimeVolume(ctx context.Context, username string) ([]*InspectorBirthTimeVolumeInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorBirthTimeVolumeInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorBirthTimeVolumeInfo(raw string) ([]*InspectorBirthTimeVolumeInfo, error) {
	inspectorBirthTimeVolumeInfos := []*InspectorBirthTimeVolumeInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "birthtime::volume") {
			continue
		}
		inspectorBirthTimeVolume, err := c.parseInspectorBirthTimeVolumeLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorBirthTimeVolumeInfos = append(inspectorBirthTimeVolumeInfos, inspectorBirthTimeVolume)
	}
	return inspectorBirthTimeVolumeInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorBirthTimeVolumeLine(line string) (*InspectorBirthTimeVolumeInfo, error) {
	kv := c.getMap(line)
	birthTimeVolumeInfo := &InspectorBirthTimeVolumeInfo{
		secondsToHumanReadable(kv["bin"]),
		kv["value"],
	}
	return birthTimeVolumeInfo, nil
}

// List Inspector Birth Time Files
func (c *Client) ListInspectorBirthTimeFiles(ctx context.Context, username string) ([]*InspectorBirthTimeFilesInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorBirthTimeFilesInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorBirthTimeFilesInfo(raw string) ([]*InspectorBirthTimeFilesInfo, error) {
	inspectorBirthTimeFilesInfos := []*InspectorBirthTimeFilesInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "birthtime::files") {
			continue
		}
		inspectorBirthTimeFiles, err := c.parseInspectorBirthTimeFilesLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorBirthTimeFilesInfos = append(inspectorBirthTimeFilesInfos, inspectorBirthTimeFiles)
	}
	return inspectorBirthTimeFilesInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorBirthTimeFilesLine(line string) (*InspectorBirthTimeFilesInfo, error) {
	kv := c.getMap(line)
	birthTimeFilesInfo := &InspectorBirthTimeFilesInfo{
		secondsToHumanReadable(kv["bin"]),
		kv["value"],
	}
	return birthTimeFilesInfo, nil
}

// List Inspector Cost Disk
func (c *Client) ListInspectorGroupCostDisk(ctx context.Context, username string) ([]*InspectorGroupCostDiskInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorGroupCostDiskInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorGroupCostDiskInfo(raw string) ([]*InspectorGroupCostDiskInfo, error) {
	inspectorGroupCostDiskInfos := []*InspectorGroupCostDiskInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "group::cost::disk") {
			continue
		}
		inspectorGroupCostDisk, err := c.parseInspectorGroupCostDiskLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorGroupCostDiskInfos = append(inspectorGroupCostDiskInfos, inspectorGroupCostDisk)
	}
	return inspectorGroupCostDiskInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorGroupCostDiskLine(line string) (*InspectorGroupCostDiskInfo, error) {
	kv := c.getMap(line)
	groupCostDiskInfo := &InspectorGroupCostDiskInfo{
		kv["groupname"],
		kv["price"],
		kv["cost"],
	}
	return groupCostDiskInfo, nil
}

// List Inspector Cost Disk
func (c *Client) ListInspectorGroupCostDiskTBYears(ctx context.Context, username string) ([]*InspectorGroupCostDiskTBYearsInfo, error) {
	unixUser, err := getUnixUser(username)
	if err != nil {
		return nil, err
	}

	ctxWt, cancel := c.getTimeout(ctx)
	defer cancel()

	cmd := exec.CommandContext(ctxWt, "/usr/bin/eos", "-r", unixUser.Uid, unixUser.Gid, "inspector", "-m")
	stdout, _, err := c.execute(cmd)
	if err != nil {
		return nil, err
	}
	return c.parseInspectorGroupCostDiskTBYearsInfo(stdout)
}

// Gathers the information of all lines.
func (c *Client) parseInspectorGroupCostDiskTBYearsInfo(raw string) ([]*InspectorGroupCostDiskTBYearsInfo, error) {
	inspectorGroupCostDiskTBYearsInfos := []*InspectorGroupCostDiskTBYearsInfo{}
	rawLines := strings.Split(raw, "\n")
	for _, rl := range rawLines {
		if rl == "" || !strings.Contains(rl, "group::cost::disk") {
			continue
		}
		inspectorGroupCostDiskTBYears, err := c.parseInspectorGroupCostDiskTBYearsLine(rl)

		if err != nil {
			return nil, err
		}
		inspectorGroupCostDiskTBYearsInfos = append(inspectorGroupCostDiskTBYearsInfos, inspectorGroupCostDiskTBYears)
	}
	return inspectorGroupCostDiskTBYearsInfos, nil
}

// Gathers information of one single line
func (c *Client) parseInspectorGroupCostDiskTBYearsLine(line string) (*InspectorGroupCostDiskTBYearsInfo, error) {
	kv := c.getMap(line)
	groupCostDiskTBYearsInfo := &InspectorGroupCostDiskTBYearsInfo{
		kv["groupname"],
		kv["tbyears"],
	}
	return groupCostDiskTBYearsInfo, nil
}
