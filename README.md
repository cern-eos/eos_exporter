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
- Run (on EOS headnode or in a pre-configured client with root privilegies on EOS)

```
./eos_exporter -eos-instance="<eos_instance>"
```

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
    - eospps.cern.ch:9986
    labels:
      instance: eospps
```
