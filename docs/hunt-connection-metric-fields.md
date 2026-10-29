# HUNT: label set for Sysdig connection metrics

Applies to `sysdig_connection_net_out_bytes` and `sysdig_connection_net_connection_out_count`.
Source of truth in code: `config.yaml` → `prometheus.aggregation.by` (38 labels) and `prometheus.aggregation.time_functions`.

## Query shape (implemented in the ingestor 2026-10-20)
- Bytes: `sum by (<38 labels>) (sum_over_time(<selector>[<step>s]))`, giving total bytes per 10s step.
  - The plain 10s value Sysdig returns is an average of roughly 1s readings: about 11–12 per 10s window, found with `count_over_time(...[10s])`.
  - Validated 2026-10-20, all three checks passed:
    1. The plain value equals `avg_over_time[10s]`.
    2. A 1-minute sum equals six 10s sums, so window boundaries aren't double-counted.
    3. The 10s sums are whole numbers.
  - The 1s readings are only kept briefly, and Sysdig's retention is inconsistent despite what they state. So this must run at fetch time, which is why the pull is hourly; older dates can't be backfilled with it.
  - The window equals the query step, so consecutive windows tile the timeline exactly.
- Connection count: a level ("currently established"), so it keeps the plain value, which is the average over 10s. Use `max_over_time` if you want peak concurrency.
  - It measures how many connections are open at once, not how many are opened: blind to short-lived bursts such as scans and beaconing.
- `sum by` was checked against the raw total: the difference is about 1e-10 relative, i.e. exact.

## Labels (38)
### Dmitri's required fields (24), plus `__name__` (re-added by the ingestor) and `time`
agent_tag_Cloud_Platform, agent_tag_TIAA_Environment, agent_tag_TIAA_Network_Segment, agent_tag_cluster,
client_ip, server_ip, remote_service, protocol, cmd_line, program_name,
container_id, container_image, container_image_digest, container_image_id, container_name,
container_label_io_kubernetes_pod_name, container_label_io_kubernetes_pod_namespace,
host_instance_id, kube_cluster_name, kube_namespace_name, kube_node_name,
kube_pod_name, kube_workload_name, kube_workload_type

### IP fields (8)
kube_pod_internal_ip (to resolve server_ip to a pod), net_client_ip, net_server_ip,
net_local_endpoint, net_remote_endpoint, local_service, net_local_service, net_remote_service

### Context (6): determined by pod, node or image, so no extra series
container_image_repo (stays the same across deploys, unlike digest/ID),
kube_workload_label_app_cmdb_id, kube_workload_label_app_family, kube_workload_label_platform_short_name,
kube_node_label_topology_kubernetes_io_zone, kube_node_label_karpenter_sh_nodepool

## Dropped (about 60)
- Duplicates of kept fields: net_protocol, program, program_cmd_line, container_full_id, container_label_io_kubernetes_container_name, pod- and workload-level container_name labels.
- Constants: _sysdig_datasource, agent_tag_on_prem, container_type, region and OS labels.
- Internal IDs: agent_id, host_id, host_mac, host_hostname, kube_cluster_id, kube_namespace_uid, kube_node_uid.
- Karpenter instance-spec node labels.
- Sparse namespace labels: istio_injection, dynakube.

## Pipeline notes
- The hourly fetch overlaps the previous one by 5 minutes, so the same series and timestamp can come from two fetches. The fetch window is snapped to the 10s step so both copies share a timestamp; the Glue job keeps one row per series and timestamp, taking the value from the later fetch (an earlier copy can be incomplete because of ingestion lag).
- Day boundary: the partition is named after the fetch end time, so the 00:00 run writes day D's last hour into dt=D+1. The Glue job reads dt=D plus the early dt=D+1 files and assigns rows to the day of their own `time` (UTC).
- Possible ingestion lag in Sysdig for the most recent ~30–40 min (not yet confirmed).
- Deploy schema changes at a day boundary so no day mixes the old (98-column) and new (41-column) layouts.
