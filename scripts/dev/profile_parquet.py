#!/usr/bin/env python3
"""Profile parquet files: which columns are really empty, and where did they come from?

Usage:
  python scripts/dev/profile_parquet.py PATH [PATH ...]      # files or dirs (recursive)
  python scripts/dev/profile_parquet.py PATH --csv out.csv   # also save the table

Per column it reports (over all rows in all given files):
  null%      real nulls / NaN
  empty%     empty-string values ("") -- Prometheus/Sysdig labels often come through as ""
  blank%     null + empty  (what you *see* as an empty column)
  nunique    distinct non-blank values
  files_with how many files contain the column at all (schema drift / mergeSchema union)
Then a per-file summary so you can tell "empty in the source file" (ingestor problem)
from "absent in some files, null after merge" (Glue mergeSchema problem).
"""
import argparse, sys
from pathlib import Path
import pyarrow.parquet as pq
import pandas as pd

ap = argparse.ArgumentParser()
ap.add_argument("paths", nargs="+")
ap.add_argument("--csv")
a = ap.parse_args()

files = []
for p in map(Path, a.paths):
    files += sorted(p.rglob("*.parquet")) if p.is_dir() else [p]
if not files:
    sys.exit("no parquet files found")

per_file, tot = [], {}
n_total = 0
for f in files:
    df = pq.read_table(f).to_pandas()
    n = len(df); n_total += n
    per_file.append((f.name, n, len(df.columns), int(sum(df[c].isna().all() or (df[c].astype(str).eq("").all()) for c in df.columns))))
    for c in df.columns:
        s = df[c]
        nul = int(s.isna().sum())
        emp = int((s.astype(str) == "").sum()) if s.dtype == object else 0
        t = tot.setdefault(c, dict(n=0, null=0, empty=0, vals=set(), files=0, dtype=str(s.dtype)))
        t["n"] += n; t["null"] += nul; t["empty"] += emp; t["files"] += 1
        if len(t["vals"]) < 1000:
            t["vals"].update(s[s.notna() & (s.astype(str) != "")].astype(str).unique()[:1000])

rows = []
for c, t in tot.items():
    # rows from files that lack the column entirely count as null after a merge
    missing = n_total - t["n"]
    rows.append(dict(column=c, dtype=t["dtype"],
        null_pct=round(100 * (t["null"] + missing) / n_total, 1),
        empty_pct=round(100 * t["empty"] / n_total, 1),
        blank_pct=round(100 * (t["null"] + missing + t["empty"]) / n_total, 1),
        nunique=len(t["vals"]) if len(t["vals"]) < 1000 else "1000+",
        files_with=f'{t["files"]}/{len(files)}'))
out = pd.DataFrame(rows).sort_values("blank_pct", ascending=False)
pd.set_option("display.width", 200, "display.max_rows", 500, "display.max_colwidth", 60)
print(f"{len(files)} file(s), {n_total} rows, {len(out)} distinct columns\n")
print(out.to_string(index=False))
print("\nColumns >=99% blank:", int((out.blank_pct >= 99).sum()),
      "| constant (nunique<=1):", int(out["nunique"].apply(lambda x: x != "1000+" and x <= 1).sum()))
print("\nPer file: name, rows, cols, fully-blank cols")
for r in per_file: print("  ", *r)
if a.csv: out.to_csv(a.csv, index=False); print("saved", a.csv)
