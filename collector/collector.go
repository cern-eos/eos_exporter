package collector

type CollectorOpts struct {
	Cluster           string
	Timeout           int
	AuditLogPath      string // Path to the audit log symlink (default: /var/log/eos/mgm/audit/audit.zstd)
	AuditPollInterval int    // Interval in seconds to check for new audit log files (default: 30)
}
