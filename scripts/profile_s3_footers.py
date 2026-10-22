#!/usr/bin/env python3
"""Profile columns across many parquet files by reading ONLY the footers (cheap).

  python scripts/profile_s3_footers.py s3://hunt-data-store/sysdig/monitor/prometheusV5/conformed/metric=sysdig_connection_net_out_bytes/cluster=eks-dev-k8s-west-2/dt=20260821/
  python scripts/profile_s3_footers.py ./local_dir          # local files work too
  options: --max-files 500  --csv cols.csv  --profile my-aws-profile

Needs: pip install boto3 pyarrow pandas   (uses your normal AWS creds / SSO session)

Per column:
  files_with     files whose schema contains the column
  rows_null_pct  null rows / ALL rows scanned (files lacking the column count as null)
  files_blank    files where the column is entirely null or entirely ""   (from footer stats)
  verdict        DROP (blank everywhere) / MOSTLY_BLANK / KEEP
A column that is blank *inside* the files that have it => ingestor problem.
A column present in few files => schema-union (mergeSchema) problem.
"""
import argparse, re, struct, sys
from collections import defaultdict
from pathlib import Path
import pyarrow as pa, pyarrow.parquet as pq, pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("path"); ap.add_argument("--max-files", type=int, default=500)
ap.add_argument("--csv"); ap.add_argument("--profile")
a = ap.parse_args()

def footer_from_tail(read_tail, size):
    t8 = read_tail(8)
    if t8[4:] != b"PAR1": raise ValueError("not parquet")
    n = struct.unpack("<I", t8[:4])[0]
    return pq.read_metadata(pa.BufferReader(b"PAR1" + read_tail(n + 8)))

items = []  # (name, callable -> metadata)
if a.path.startswith("s3://"):
    import boto3
    s3 = (boto3.Session(profile_name=a.profile) if a.profile else boto3.Session()).client("s3")
    bkt, pre = re.match(r"s3://([^/]+)/?(.*)", a.path).groups()
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bkt, Prefix=pre):
        for o in page.get("Contents", []):
            k = o["Key"]
            if k.endswith(".parquet") or ("/_" not in k and "." not in k.rsplit("/", 1)[-1]):
                if o["Size"] > 12:
                    items.append((k, (lambda k=k: footer_from_tail(
                        lambda n: s3.get_object(Bucket=bkt, Key=k, Range=f"bytes=-{n}")["Body"].read(), 0))))
        if len(items) >= a.max_files: break
else:
    p = Path(a.path)
    for f in (sorted(p.rglob("*.parquet")) if p.is_dir() else [p]):
        items.append((str(f), (lambda f=f: pq.read_metadata(f))))
items = items[: a.max_files]
if not items: sys.exit("no parquet files found")

total_rows = 0
cols = defaultdict(lambda: dict(files=0, null=0, rows=0, blank_files=0, dtype=""))
errs = 0
for i, (name, get) in enumerate(items, 1):
    try: md = get()
    except Exception as e:
        errs += 1; print("skip", name, e, file=sys.stderr); continue
    total_rows += md.num_rows
    sch = md.schema.to_arrow_schema()
    for ci, field in enumerate(sch):
        c = cols[field.name]; c["files"] += 1; c["rows"] += md.num_rows; c["dtype"] = str(field.type)
        nulls, allblank, ok = 0, True, True
        for rg in range(md.num_row_groups):
            cc = md.row_group(rg).column(ci); st = cc.statistics
            if pa.types.is_null(field.type):          # all-null column: no stats written
                nulls += md.row_group(rg).num_rows; continue
            if st is None or not st.has_null_count: ok = False; break
            nulls += st.null_count
            nn = cc.num_values - st.null_count
            if nn > 0:
                if st.has_min_max and st.min == "" and st.max == "": pass   # all-empty-string
                else: allblank = False
        if ok:
            c["null"] += nulls
            if allblank: c["blank_files"] += 1
    if i % 100 == 0: print(f"...{i}/{len(items)}", file=sys.stderr)

nf = len(items) - errs
rows = []
for name, c in cols.items():
    null_pct = 100 * (c["null"] + (total_rows - c["rows"])) / max(total_rows, 1)
    verdict = "DROP" if c["blank_files"] == c["files"] and c["files"] > 0 else ("MOSTLY_BLANK" if null_pct >= 90 else "KEEP")
    rows.append(dict(column=name, dtype=c["dtype"], files_with=f'{c["files"]}/{nf}',
                     rows_null_pct=round(null_pct, 1), files_blank=c["blank_files"], verdict=verdict))
df = pd.DataFrame(rows).sort_values(["verdict", "rows_null_pct"], ascending=[True, False])
pd.set_option("display.width", 220, "display.max_rows", 2000, "display.max_colwidth", 70)
print(f"\n{nf} files, {total_rows} rows, {len(df)} distinct columns ({errs} unreadable)\n")
print(df.to_string(index=False))
print("\n", df.verdict.value_counts().to_string())
if a.csv: df.to_csv(a.csv, index=False); print("saved", a.csv)
