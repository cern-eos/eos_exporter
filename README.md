# eos_exporter
[CERN](https://home.cern/) [EOS](https://eos.web.cern.ch) metrics exporter for Prometheus

## Usage

- Compile ([Go](https://golang.org/doc/install) environment needed)

```
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
- For more options, use `--help`

## Prometheus example configuration

```
- job_name: eos
  scrape_interval: 30s
  static_configs:
  - targets:
    - eosheadnode.domain.com:9986
```

## Troubleshooting

This tool is provided by CERN EOS Operators. Report issues on Github tracker or contact us through the [EOS community forum](https://eos-community.web.cern.ch/)