# eos_exporter
CERN [EOS](https://eos.web.cern.ch) metrics exporter for Prometheus

## Usage

- Compile ([Go](https://golang.org/doc/install) environment needed)

```
go build
```
- Run (on EOS headnode or in a pre-configured client with root privilegies on EOS)

```
./eos_exporter --eos-instance="<eos_instance>"
```

- By default, the exporter exposes the metrics on the port `9373` and url `/metrics`. 
    - Change the port with the argument `--web.listen-address` 
    - Change the url with `--web.telemetry-path`
- For more options, use `--help`

## Prometheus example configuration

```
- job_name: eos
  scrape_interval: 30s
  static_configs:
  - targets:
    - eospps.cern.ch:9373
    labels:
      cluster: eospps
```
