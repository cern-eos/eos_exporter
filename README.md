# eos_exporter
[CERN](https://home.cern/) [EOS](https://eos.web.cern.ch) metrics exporter for Prometheus

## Usage

- Compile ([Go](https://golang.org/doc/install) >=1.18 environment needed)

```
cd eos_exporter
./get_build_info.sh
go build
```

- There is also a Makefile available that can be launched in the following way:
```
make build
```
- Run on EOS headnode.

```
./eos_exporter -eos-instance="<eos_instance>"
```
> This variable is used to populate internal `cluster` label. Will be deprecated, global labels can serve the same purpose. 
> Actual MGM to connect is gathered from EOS_MGM_URL in EOS configuration.

- By default, the exporter exposes the metrics on the port `9986` and url `/metrics`. 
    - Change the port with the argument `-listen-address`
    - Change the url with `-telemetry-path`
- The deprecated fast metrics exporter is disabled by default.
    - Enable it with `-enable-fast-exporter`
    - Change its port with `-listen-address-fast`
- For more options, use `--help`

## Prometheus example configuration

```
- job_name: eos
  scrape_interval: 30s
  static_configs:
  - targets:
    - eosheadnode.domain.com:9986
```

## CERN Grafana Dashboard

We are providing the dashboard that we use in CERN instances. It is provided `as is`, so some modifications would be needed to adapt to external deployments.
The dashboard expects a variable called `instance` that is used to filter using the `cluster` label. Create the variable in Grafana using the query `label_values(cluster)`.
It also includes plots for node_exporter metrics, if available. 

## Troubleshooting

This tool is provided by CERN EOS Operators. Report issues on Github tracker or contact us through the [EOS community forum](https://eos-community.web.cern.ch/)

## Traffic-shaping dashboard compatibility on EOS 5.4

The `traffic_shaping_io` collector consumes the cumulative counter snapshot from
`eos io shaping ls --apps --json --sys`. It requires the corresponding EOS 5.4
monitoring patch (`engine_meta.counters.version = 1`). It does not require a new
console client, `--all`, filesystem detail, or native Prometheus support in EOS.
The EOS shaping collector must already be enabled. This exporter does not enable
it or change enforcement/policies.

Shaping IO and policy metrics remain on the separate fast endpoint. Enable it
with `--enable-fast-exporter` and scrape port `9987` to collect these metrics.
The standard endpoint keeps its existing collectors, including shaping
configuration and the leader MGM version. An unsupported counter snapshot
reports `eos_io_shaping_scrape_success=0` on the fast endpoint. The fast
shaping collectors log a failure once until collection succeeds again.

For the supplied Grafana dashboard, set Prometheus `scrape_interval` to 15s
and Grafana's Prometheus data-source scrape interval to the same value.

The collector replaces its old windowed rate gauges with genuine counters:

- `eos_io_shaping_bytes_total` and `eos_io_shaping_operations_total`, with
  `cluster`, `type`, `id`, `operation` labels (`type=app|uid|gid|node`).
- `eos_io_shaping_all_bytes_total` and `eos_io_shaping_all_operations_total`,
  with the native exporter's node/application/user/group labels, including
  numeric `uid_id`/`gid_id` and resolved `id(name)` labels.

Existing dashboard `rate(...[$__rate_interval])` queries therefore work without
rate-gauge fallbacks. Overview, rankings, node/application/user/group drill-downs,
policies, loop timings and report-processing panels are supported. Counter values
come directly from EOS; exporter restarts do not reset them, and MGM resets are
preserved for Prometheus to detect. A one-second cache shares snapshots between
concurrent scrapes. Refresh failure removes stale traffic samples and sets
`eos_io_shaping_scrape_success=0`; unsupported EOS releases fail explicitly.

`eos_monit_enabled=1` advertises this exporter's monitoring interface so existing
dashboard instance selectors work. It does **not** imply shaping enforcement is
enabled. `eos_monit_cache_ttl_seconds` describes the counter snapshot cache.

Filesystem detail is intentionally unavailable: `fsid="0"` is the aggregate
placeholder, and no `eos_io_shaping_fs_*` counters are fabricated. Leave the
filesystem selector on All. Filesystem activity/count panels may show this one
placeholder bucket; they do not measure physical filesystems. Newer controller,
pressure and internal-map diagnostics are also unavailable. Do not scrape native
and external shaping counters into the same dashboard selection simultaneously.

EOS retains at most 50,000 identities and an estimated 64 MiB of counter state.
Watch `eos_io_shaping_counter_entries_rejected_total` and
`eos_io_shaping_all_entries_limited`; nonzero values indicate incomplete traffic
coverage. Existing identities continue counting when admission is limited.

Validation:

```sh
./get_build_info.sh
go test -race ./...
go build
promtool test rules testdata/shaping-dashboard.test.yml
```
