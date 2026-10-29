# ./src/hunt/prometheus_tools/transform.py
"""Prometheus range-query JSON -> DataFrame, plus the data-quality counters for the job event."""

import re
from logging import Logger
from typing import List, Optional, Sequence, Tuple

import numpy as np
import pandas as pd

from hunt.prometheus_tools.utils import drop_sparse_columns, get_app_cmdb_cols

CMDB_RE = re.compile(r"label_A\d{5,}")


def series_stats(values: pd.Series, start: int, stop: int, step: int) -> dict:
    """Sample counts, gaps and lag, from the per-series [[ts, "v"], ...] lists."""
    expected = (stop - start) // step + 1
    # Last timestamp a range query can return: start + k*step <= stop
    last_possible = start + (expected - 1) * step

    lens = values.map(len)
    first = values.map(lambda v: float(v[0][0]))
    last = values.map(lambda v: float(v[-1][0]))
    # Points missing between a series' own first and last sample. Short-lived connections
    # naturally don't span the whole window, so that alone is not counted as a gap.
    span_points = ((last - first) / step).round().astype("int64") + 1
    missing = (span_points - lens).clip(lower=0)
    last_ts = int(last.max())

    return {
        "samples": int(lens.sum()),
        "expected_samples_per_series": int(expected),
        "series_full_window": int((lens >= expected).sum()),
        "series_with_gaps": int((missing > 0).sum()),
        "missing_points": int(missing.sum()),
        "first_ts": int(first.min()),
        "last_ts": last_ts,
        "lag_s": int(last_possible - last_ts),
    }


def conform_aggregated(
    df: pd.DataFrame, metric_name: str, labels: Sequence[str], logger: Logger
) -> Tuple[pd.DataFrame, List[str]]:
    """Fixed schema for aggregated metrics: same columns, order and types in every file.

    Prometheus omits labels whose value is empty, so a label can be absent for a whole
    fetch; add it as an all-null *string* column so parquet types never drift between
    files (an all-null object column would be written as Arrow `null` and the Glue
    schema check would skip the file).

    Returns the conformed frame and the allowlisted labels that were null for the whole fetch.
    """
    label_cols = [f"metric.{lbl}" for lbl in labels]
    df["metric.__name__"] = metric_name  # aggregation drops __name__
    extra = sorted(set(df.columns) - set(label_cols) - {"metric.__name__", "time", "value"})
    if extra:
        logger.warning(
            "Unexpected columns after aggregation dropped",
            extra={"event": "unexpected_columns_dropped", "dropped_columns": extra},
        )
    df = df.reindex(columns=["metric.__name__", *label_cols, "time", "value"])
    null_labels = [lbl for lbl, col in zip(labels, label_cols) if df[col].isna().all()]
    str_cols = ["metric.__name__", *label_cols]
    df[str_cols] = df[str_cols].astype("string")
    return df, null_labels


def drop_unwanted_columns(df: pd.DataFrame, logger: Logger) -> pd.DataFrame:
    """Heuristic column filter for metrics fetched without aggregation: sparse and CMDB columns."""
    cols_before = len(df.columns)
    df = drop_sparse_columns(df, na_ratio=0.1)
    cols_dropped = cols_before - len(df.columns)

    if cols_dropped > 0:
        logger.debug(
            "Dropped sparse columns",
            extra={"event": "sparse_columns_dropped", "count": cols_dropped},
        )

    cmdb_cols = get_app_cmdb_cols(df, CMDB_RE)
    if cmdb_cols:
        df = df.drop(columns=cmdb_cols)
        logger.debug(
            "Dropped CMDB columns", extra={"event": "cmdb_columns_dropped", "count": len(cmdb_cols)}
        )

    return df


def response_to_frame(
    raw: dict,
    *,
    metric_name: str,
    start: int,
    stop: int,
    step: int,
    logger: Logger,
    labels: Optional[Sequence[str]] = None,
    time_function: Optional[str] = None,
) -> Tuple[pd.DataFrame, dict]:
    """Prometheus range-query JSON -> (DataFrame, data-quality counters).

    `labels` is the aggregation allowlist for aggregated metrics (fixed schema); None means the
    metric was fetched raw and its columns are filtered heuristically.
    """
    quality = {}

    result_list = raw.get("data", {}).get("result", [])
    if not result_list:
        return pd.DataFrame(), quality

    df = pd.json_normalize(result_list)

    if df.empty:
        logger.debug("DataFrame empty after normalization", extra={"event": "normalize_empty"})
        return pd.DataFrame(), quality

    quality.update(series_stats(df["values"], start, stop, step))
    df = df.explode("values", ignore_index=True)

    v = np.asarray(df["values"].tolist(), dtype="object")
    if v.size == 0:
        logger.debug("No values to process after explosion", extra={"event": "explode_empty"})
        return pd.DataFrame(), quality

    df["time"] = pd.to_datetime(v[:, 0], unit="s", utc=True, errors="coerce")
    df["value"] = pd.to_numeric(v[:, 1], errors="coerce").astype("float64")
    df = df.drop(columns=["values"])

    # Values/timestamps that could not be parsed (or were "NaN" in the payload)
    quality["nan_values"] = int(df["value"].isna().sum())
    quality["nat_times"] = int(df["time"].isna().sum())
    if time_function == "sum_over_time":
        # sum_over_time of ~1s readings must be whole numbers; anything else means averaged data
        values = df["value"].dropna().to_numpy()
        quality["non_integer_values"] = int(np.count_nonzero(np.mod(values, 1)))

    if labels is not None:
        df, null_labels = conform_aggregated(df, metric_name, labels, logger)
        quality["null_label_cols"] = null_labels
        quality["null_label_count"] = len(null_labels)
        return df, quality

    return drop_unwanted_columns(df, logger), quality
