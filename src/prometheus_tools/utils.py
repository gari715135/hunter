# ./src/prometheus_tools/utils.py
import io
import re
from datetime import datetime, timezone

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq


def get_app_cmdb_cols(df: pd.DataFrame, pattern: re.Pattern) -> list:
    return [c for c in df.columns if pattern.search(c)]


def drop_sparse_columns(df: pd.DataFrame, *, na_ratio: float = 0.95) -> pd.DataFrame:
    if df.empty:
        return df

    # Minimum number of non-NA rows a column must have to be kept
    thresh = max(1, int(np.ceil(len(df) * (1 - na_ratio))))
    return df.dropna(axis=1, thresh=thresh)


def get_timestamp_parts(timestamp: int) -> tuple:
    dt = datetime.fromtimestamp(timestamp, timezone.utc)
    folder = dt.strftime("%Y%m%d")
    fname = dt.strftime("%H%M")
    return folder, fname


def df_to_parquet_bytes(df: pd.DataFrame, *, compression="snappy") -> bytes:
    table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(
        table,
        buf,
        compression=compression,
        use_dictionary=True,
        coerce_timestamps="us",
        allow_truncated_timestamps=True,
    )
    buf.seek(0)
    return buf.getvalue()