# ./scripts/inject_beacon.py
"""Add a synthetic beacon to a copy of a real parquet partition, to check detect_beacons flags it.

Reads one metric/cluster partition (a file or a directory), appends one client/server pair
that spikes every --period seconds for the whole time range of the data, and writes the
result under --out. The metric=/cluster=/dt= folders of the input path are kept, so
detect_beacons still tags its log events with the metric and cluster.

    python scripts/inject_beacon.py ~/beacon-test/metric=M/cluster=C/dt=D --out ~/beacon-test/injected

Then run detect_beacons with BEACON_INPUT set to the path it prints. The injected pair should
come out with is_suspicious=true and peak_period_seconds close to --period. Its rows carry
metric.kube_pod_name=injected-beacon so they are easy to find.
"""
import argparse
import re
from pathlib import Path

import numpy as np
import pandas as pd

INJECTED_POD = "injected-beacon"


def beacon_rows(
    template: pd.DataFrame,
    client_ip: str,
    server_ip: str,
    period: int,
    step: int,
    spike: float,
    baseline: float,
    jitter: int,
    seed: int,
    client_col: str = "metric.client_ip",
    server_col: str = "metric.server_ip",
    time_col: str = "time",
    value_col: str = "value",
) -> pd.DataFrame:
    """One row per step over the template's time range: `spike` every `period` seconds, else `baseline`.

    Each spike is moved by up to +/- `jitter` seconds (rounded to the step), like a real implant's
    sleep jitter. Every other column is left empty except the cluster name and the marker pod name.
    """
    times = pd.to_datetime(template[time_col], utc=True)
    grid = pd.date_range(times.min().floor(f"{step}s"), times.max(), freq=f"{step}s")
    values = np.full(len(grid), baseline, dtype=float)

    rng = np.random.default_rng(seed)
    offsets = np.arange(0, len(grid) * step, period)
    if jitter:
        offsets = offsets + rng.integers(-jitter, jitter + 1, size=len(offsets))
    idx = np.round(offsets / step).astype(int)
    values[idx[(idx >= 0) & (idx < len(grid))]] = spike

    rows = pd.DataFrame(
        {c: pd.Series([None] * len(grid), dtype=template[c].dtype) for c in template.columns}
    )
    rows[time_col] = grid if template[time_col].dt.tz is not None else grid.tz_localize(None)
    rows[client_col] = client_ip
    rows[server_col] = server_ip
    rows[value_col] = values
    if "metric.kube_pod_name" in rows.columns:
        rows["metric.kube_pod_name"] = INJECTED_POD
    if (
        "metric.kube_cluster_name" in rows.columns
        and template["metric.kube_cluster_name"].notna().any()
    ):
        rows["metric.kube_cluster_name"] = template["metric.kube_cluster_name"].mode().iloc[0]
    return rows


def partition_suffix(path: str) -> Path:
    """The key=value folders of a hive-partitioned path (metric=/cluster=/dt=), in order."""
    return Path(
        *[part for part in Path(path).parts if re.fullmatch(r"[^=/]+=[^/]+", part)] or ["."]
    )


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("input", help="parquet file or directory for one metric/cluster")
    parser.add_argument(
        "--out",
        required=True,
        help="directory to write under (input's metric=/cluster=/dt= folders are kept)",
    )
    parser.add_argument("--client-ip", default="10.250.250.250")
    parser.add_argument(
        "--server-ip", default="203.0.113.10", help="must be public (default: TEST-NET-3)"
    )
    parser.add_argument(
        "--period", type=int, default=60, help="seconds between spikes (default: 60)"
    )
    parser.add_argument(
        "--step", type=int, default=10, help="sample step of the data, seconds (default: 10)"
    )
    parser.add_argument(
        "--spike",
        type=float,
        default=1.0,
        help="value at each check-in (default: 1, one connection)",
    )
    parser.add_argument(
        "--baseline", type=float, default=0.0, help="value between check-ins (default: 0)"
    )
    parser.add_argument(
        "--jitter",
        type=int,
        default=0,
        help="max seconds each spike is moved early/late (default: 0)",
    )
    parser.add_argument("--seed", type=int, default=0)
    args = parser.parse_args(argv)

    if args.period < 2 * args.step:
        parser.error(
            "--period must be at least twice --step, or there is no repeating pattern to see"
        )

    original = pd.read_parquet(args.input)
    if original.empty:
        parser.error(f"{args.input} has no rows")
    injected = beacon_rows(
        original,
        args.client_ip,
        args.server_ip,
        args.period,
        args.step,
        args.spike,
        args.baseline,
        args.jitter,
        args.seed,
    )
    combined = pd.concat([original, injected], ignore_index=True)

    out_dir = Path(args.out) / partition_suffix(args.input)
    out_dir.mkdir(parents=True, exist_ok=True)
    combined.to_parquet(out_dir / "part-0.parquet", index=False)

    print(
        f"Added {len(injected)} rows for {args.client_ip} -> {args.server_ip}, "
        f"spike every {args.period}s (jitter +/-{args.jitter}s), {len(original)} original rows kept"
    )
    print(f"BEACON_INPUT={out_dir}")


if __name__ == "__main__":
    main()