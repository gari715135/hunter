# ./scripts/glue_expected_schema.py
"""Print the EXPECTED_SCHEMA_JSON job arg for glue_combine_conformed.py.

The schema is taken from a parquet file written by the ingestor's own transform and writer,
using the labels in config.yaml (prometheus.aggregation.by), so it always matches the files.

    python scripts/glue_expected_schema.py
"""
import argparse
import io
import json
import logging
import sys
from pathlib import Path

PROJECT_ROOT = str(Path(__file__).resolve().parent.parent)
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

import pyarrow.parquet as pq
import yaml

from src.prometheus_tools.promql import Aggregation
from src.prometheus_tools.transform import response_to_frame
from src.prometheus_tools.utils import df_to_parquet_bytes


def expected_schema(cfg: dict) -> dict:
    """{column: arrow type} of a conformed parquet file for an aggregated metric."""
    agg = Aggregation.from_config(cfg.get("prometheus", {}).get("aggregation"))
    if not agg.metrics:
        raise ValueError("prometheus.aggregation.metrics is empty: there is no fixed schema to pin")
    metric = sorted(agg.metrics)[0]
    raw = {"data": {"result": [{"metric": {lbl: "x" for lbl in agg.by}, "values": [[0, "1"]]}]}}
    df, _ = response_to_frame(
        raw, metric_name=metric, start=0, stop=0, step=10, logger=logging.getLogger(__name__), labels=agg.by
    )
    schema = pq.read_schema(io.BytesIO(df_to_parquet_bytes(df)))
    return {f.name: str(f.type).replace("large_string", "string") for f in schema}


def main(argv=None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--config", default=str(Path(PROJECT_ROOT) / "config.yaml"))
    args = parser.parse_args(argv)
    with open(args.config) as f:
        cfg = yaml.safe_load(f)
    print(json.dumps(expected_schema(cfg), separators=(",", ":")))


if __name__ == "__main__":
    main()
