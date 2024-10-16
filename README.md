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
- For more options, use `--help`

## Tagging
1. To tag a new version, update the `eos_exporter.spec` file with the following:
   - `%define version 0.1.14` (aka next tag version)
   - Add your commits/changes in the changelog with this format:

     ```plaintext
     %changelog
     * Wed Oct 16 2024 Maria Arsuaga Rios <maria.arsuaga.rios@cern.ch> 0.1.14-1
     - Adding EC categories for fsck
     ```

2. Create a pull request for your branch: [https://github.com/cern-eos/eos_exporter/pulls](https://github.com/cern-eos/eos_exporter/pulls)

3. When the reviewer approves your pull request, click **Rebase and Merge**.

4. Meanwhile, create the corresponding tag in the `master` branch (ensure `git pull` first):
   ```bash
   git tag -d v0.1.14
   git push --tags
   
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
