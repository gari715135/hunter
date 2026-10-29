# ./scripts/dev/show_query.py
"""Print the PromQL the pipeline sends for a metric/cluster, plus looser variants to paste into Sysdig.

No token or network needed: the query is built from config.yaml with the same code the ingestor uses.

    python scripts/dev/show_query.py ocp-t3-gen-prodb sysdig_connection_net_connection_out_count
    python scripts/dev/show_query.py --job-id sysdig_connection_net_connection_out_count:ocp-t3-gen-prodb
"""
import argparse
import sys
from pathlib import Path

SRC_DIR = str(Path(__file__).resolve().parents[2] / "src")
if SRC_DIR not in sys.path:
    sys.path.insert(0, SRC_DIR)

import yaml

from hunt.config_loader import default_config_path
from hunt.prometheus_tools.promql import Aggregation, cluster_selector

ALL_CLUSTERS = "all_clusters"


def build_queries(cfg: dict, metric: str, cluster: str, step_seconds: int) -> dict:
    """Return {label: promql}, from the exact pipeline query down to the bare metric."""
    prom_conf = cfg.get("prometheus", {})
    agg = Aggregation.from_config(prom_conf.get("aggregation"))
    excludes = (prom_conf.get("exclude_namespaces") or [], prom_conf.get("exclude_containers") or [])
    all_clusters = cluster == ALL_CLUSTERS
    queries = {
        "pipeline query (as sent)": agg.wrap(cluster_selector(metric, None if all_clusters else cluster, *excludes), metric, step_seconds),
    }
    if not all_clusters:
        queries["no aggregation"] = cluster_selector(metric, cluster, *excludes)
        queries["cluster only (no namespace/container exclusions)"] = f'{metric}{{agent_tag_cluster="{cluster}"}}'
    queries["metric exists at all"] = f"count({metric})"
    queries["clusters reporting this metric"] = f"count by (agent_tag_cluster) ({metric})"
    return queries


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("cluster", nargs="?", help=f"cluster name, or {ALL_CLUSTERS}")
    parser.add_argument("metric", nargs="?")
    parser.add_argument("--job-id", help="'<metric>:<cluster>' exactly as in the log line's job_id")
    parser.add_argument("--config", default=default_config_path())
    parser.add_argument("--step", type=int, default=10, help="query step in seconds (default 10, matches hunt.cli.fetch)")
    args = parser.parse_args(argv)
    if args.job_id:
        args.metric, _, args.cluster = args.job_id.partition(":")
    if not (args.metric and args.cluster):
        parser.error("give '<cluster> <metric>' or --job-id")
    return args


def main(argv=None) -> None:
    args = parse_args(argv)
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    for label, query in build_queries(cfg, args.metric, args.cluster, max(args.step, 10)).items():
        print(f"# {label}\n{query}\n")


if __name__ == "__main__":
    main()
