# HUNT: project brief

Handoff document for an agent starting on this project with no prior context. Last updated 2026-10-22.

## What this is
HUNT is a network threat-detection data pipeline for Kubernetes, plus the detectors that consume its data. It covers about 60 clusters, mostly AWS EKS, with some OpenShift and some non-AWS clusters. Sysdig's Prometheus-compatible API supplies per-connection network metrics. These are landed in S3 as Parquet for detection work, and the data is also delivered to a downstream consumer, Dmitri, who defined the required field list.

**Data flow:** Sysdig Prometheus API → `prom_ingestor.py` (hourly fetch) → S3, Hive-partitioned as `metric=/cluster=/dt=` → AWS Glue job (`glue_combine_conformed.py`) → combined Parquet → detectors.

**Metrics:** `sysdig_connection_net_out_bytes` and `sysdig_connection_net_connection_out_count`, at 10-second resolution. `connection_request_count` is also fetched raw.

**Detectors (separate from the pipeline):**
- **Beacon/C2 detector (FFT-based).** `beacon_detector.py` replaced an inefficient inherited hierarchical drill-down that diluted signal and had a peak-frequency bug. It groups traffic by `(client_ip, server_ip)` and keeps only public destinations, using one global RFC1918+CGNAT filter. It runs an FFT for periodicity and reuses `SignalTransformations`. Legitimate periodic k8s traffic (Prometheus, KEDA, CSI drivers, health probes) is a known false-positive source. A baseline/allowlist of benign periodic patterns is being worked on.
- **Bollinger-band detector.** This does per-connection volume baselining and is its own process.

The original detection goal was catching SYN-level activity such as scans. It doesn't have to be a raw packet count.

## Repo layout
- `config.yaml` is the source of truth for `prometheus.aggregation.by` (38 labels) and `prometheus.aggregation.time_functions`.
- `src/prometheus_tools/` holds `prom_ingestor.py`, `utils.py` and `orchestrator.py`.
- `scripts/` holds `fetch_data.py`, `glue_combine_conformed.py`, and the profiling scripts `profile_parquet.py` and `profile_s3_footers.py` (the latter reads only Parquet footers, so it is cheap across many S3 files).
- `docker/` holds two separate Dockerfiles, one for fetch and one for the beacon detector. Build with `docker build -f`. The detector image follows the org's corporate proxy and IMDS-bypass conventions.
- `.bak` files hold the originals from before any changes, and `.bak2` files hold the versions from before the 2026-10-20 query change.
- `hunt-connection-metric-fields.md` holds the full label list, query shape and validation results.
- On the EC2 host, code lives in `/opt/hunt`, a setgid shared directory with group `hunt`. It is synced from the dev machine through S3 with `rsync --delete --no-owner --no-group --chmod=Dg+rwxs,Fg+rw`.

## What has been done
1. **Beacon detector built.** It was rebuilt leaf-first, as described above, with JSON-line logging for Splunk. Its entry point lives in `scripts/`. It can read the S3 Parquet through DuckDB, one cluster at a time.
2. **Empty-column investigation.** Many columns in the S3 output looked empty. Two causes were suspected: Glue's `mergeSchema` unioning columns across files, and empty-string labels slipping past the ingestor's sparse-column filter. The profiling scripts were written to tell these apart. Profiling real S3 data across multiple days showed a stable 98-column schema with no truly empty columns, but many labels were useless.
3. **Switch to an allowlist.** Filtering moved from the ingestor's heuristics to an explicit allowlist applied at the source with PromQL `sum by (...)`. The allowlist is 38 labels:
   - Dmitri's 24 required fields.
   - 8 IP-bearing fields: `kube_pod_internal_ip`, `net_client_ip`, `net_server_ip`, `net_local_endpoint`, `net_remote_endpoint`, `local_service`, `net_local_service` and `net_remote_service`.
   - 6 context labels, which add no extra series because they are fixed by the pod, node or image.

   About 60 other labels were dropped as duplicates, constants, internal IDs or sparse. Output is now 41 columns, down from 98.
4. **Understanding what the data means.**
   - Plain 10s values from Sysdig are averages of roughly 11-12 one-second readings. They are not totals.
   - `sum_over_time(...[10s])` gives true bytes per 10s. Three checks passed on 2026-10-20: the plain value equals `avg_over_time`, a 1-minute sum equals six 10s sums, and the 10s sums are whole numbers.
   - The connection-count metric is a concurrency level ("currently established"), not a count of connection opens. It is blind to short-lived bursts such as scans and beaconing. It keeps the plain value, and `max_over_time` can be used for peak concurrency.
   - `sum by` was checked against raw totals and is exact to about 1e-10 relative.
5. **Query change implemented (2026-10-20).**
   - Bytes now use `sum by (<38 labels>) (sum_over_time(<selector>[<step>s]))`.
   - The window always equals the query step, so windows tile the timeline.
   - `time_functions` in config names the function per metric. Unsupported functions such as `rate`, or metrics missing from the aggregation list, fail at startup.
   - It was tested with stubs only and has not been run against the live Sysdig API.
6. **Alternatives researched.** Sysdig's metadata endpoint (about 9,900 metrics) was scanned, and external options were reviewed: Istio/Envoy TCP metrics already in the same instance, cAdvisor counters, node_exporter/conntrack, EKS Container Network Observability, Retina, Cilium/Hubble, and VPC Flow Logs. Some SYN-oriented sources were ruled out. Socket-level sources can't see raw-socket SYN scans, and VPC Flow Logs don't cover non-AWS clusters.

## Current state and next steps
The code changes are in place but unverified live. In order:
1. Run one live fetch with DEBUG logging and confirm the logged query.
2. Run the decimals check on that fetch's bytes output. It should be all whole numbers.
3. Do the Glue work:
   - Drop the duplicate rows from the deliberate 5-minute fetch overlap with `dropDuplicates()` on all columns.
   - Handle the day-boundary partitioning.
   - Deploy the schema change at a day boundary so no day mixes the old 98-column and new 41-column layouts.

## Constraints and gotchas
- **10s resolution is required for now.** It may be scaled back later.
- **Hourly pulls are mandatory.** Sysdig's 1s readings and its retention are shorter and less consistent than documented, and this has been raised with Sysdig without a fix. The fetch-time aggregation can't be backfilled for older dates.
- **The 5-minute overlap between fetches is intentional.** It minimizes gaps. Deduplicate downstream, not at the source.
- **Day boundary.** The partition is named for the fetch end time, so the 00:00 run writes day D's last hour into `dt=D+1`.
- **Ingestion lag.** Sysdig may lag for the most recent 30-40 minutes. This is unconfirmed.
- **`*_over_time` drops the metric name.** The ingestor re-adds `__name__`, so this is expected. The Sysdig console shows "Query did not return any labels" for aggregated queries. To see labels, run the query unwrapped.
- **Uneven metric coverage.** Some metrics, such as node_exporter-based ones, are missing on some clusters.
- **Memory limits.** Loading all clusters at once into pandas runs out of memory, so iterate per cluster. Use `%dev%` as the DuckDB `LIKE` pattern to filter dev clusters.
- **S3 credentials.** Under SSO, `boto3.Session().get_credentials()` is more reliable than DuckDB's `credential_chain`.
- **Sysdig may eventually be replaced, but not soon.** A vendor-neutral record would make a switch easier: time bucket, source pod/workload/namespace/IP, destination IP/port/protocol, bytes, connection count and data source, with a thin adapter per source.

## Open questions
- Which alternative source, if any, can serve SYN/scan detection better than `connection_*`. Candidates should be run through the same checks used so far: column profiling, duplicate detection, the decimals test and gap analysis.
- How to reliably classify inside versus outside the cluster. `net_remote_endpoint` resolving to a name suggests internal, but that is only a heuristic. Sysdig's real classification is CIDR-driven and lives in Secure.
- How to suppress legitimate periodic traffic in the beacon detector.
