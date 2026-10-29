# ./src/hunt/prometheus_tools/promql.py
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Iterable, Optional, Tuple

TIME_FUNCTIONS = {
    "sum_over_time",
    "avg_over_time",
    "max_over_time",
    "min_over_time",
    "count_over_time",
}


def cluster_selector(
    metric_name: str,
    cluster: Optional[str],
    exclude_namespaces: Iterable[str] = (),
    exclude_containers: Iterable[str] = (),
) -> str:
    """Selector for one cluster with the namespace/container exclusions; bare metric when cluster is None.

    The exclusions come from prometheus.exclude_namespaces / exclude_containers in config.yaml.
    """
    if cluster is None:
        return metric_name
    matchers = [f'agent_tag_cluster="{cluster}"']
    matchers += [f'kube_namespace_name!="{ns}"' for ns in exclude_namespaces]
    matchers += [f'container_name!="{cn}"' for cn in exclude_containers]
    return f"{metric_name}{{{','.join(matchers)}}}"


@dataclass(frozen=True)
class Aggregation:
    """Server-side aggregation settings (prometheus.aggregation in config.yaml)."""

    op: str = "sum"
    by: Tuple[str, ...] = ()
    metrics: FrozenSet[str] = frozenset()
    time_functions: Dict[str, str] = field(default_factory=dict)

    @classmethod
    def from_config(cls, agg: Optional[dict]) -> "Aggregation":
        agg = agg or {}
        spec = cls(
            op=agg.get("op", "sum"),
            by=tuple(agg.get("by") or []),
            metrics=frozenset(agg.get("metrics") or []),
            time_functions=dict(agg.get("time_functions") or {}),
        )
        if spec.metrics and not spec.by:
            raise ValueError("prometheus.aggregation.metrics is set but aggregation.by is empty")
        bad = {
            m: f
            for m, f in spec.time_functions.items()
            if f not in TIME_FUNCTIONS or m not in spec.metrics
        }
        if bad:
            raise ValueError(
                f"Invalid aggregation.time_functions (unknown function or metric not in aggregation.metrics): {bad}"
            )
        return spec

    def applies_to(self, metric_name: Optional[str]) -> bool:
        return metric_name in self.metrics

    def time_function(self, metric_name: Optional[str]) -> Optional[str]:
        return self.time_functions.get(metric_name)

    def wrap(self, q: str, metric_name: str, step_seconds: int) -> str:
        """`<op> by (<by>) (<q>)`, or `<op> by (<by>) (<fn>(<q>[<step>s]))` with a time function."""
        if not self.applies_to(metric_name):
            return q
        fn = self.time_function(metric_name)
        if fn:
            # Window == step so consecutive windows tile the timeline exactly.
            q = f"{fn}({q}[{step_seconds}s])"
        return f"{self.op} by ({', '.join(self.by)}) ({q})"
