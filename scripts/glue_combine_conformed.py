# glue_combine_conformed.py
# Fault-tolerant version: inspects every parquet footer before reading,
# skips files whose column types drift from the canonical schema,
# writes a drift report, and fails loudly if too much data would be dropped.
# Rows are assigned to the day of their own timestamp (not the fetch's dt folder) and
# the overlap between consecutive hourly fetches is deduplicated, keeping the later fetch.
import sys
import re
import json
import struct
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

args = getResolvedOptions(
   sys.argv, ["JOB_NAME", "SOURCE_BASE_PATH", "OUTPUT_BASE_PATH"]
)


def optional_arg(name, default):
   """Glue raises GlueArgumentError (an Exception) when an arg is missing."""
   try:
       return getResolvedOptions(sys.argv, [name])[name]
   except Exception:
       return default


process_date = optional_arg(
   "PROCESS_DATE",
   (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y%m%d"),
)
# Folders are named for the fetch END time, so day D's last hour sits in dt=D+1
# (the 00:00 fetch, and the 5-min overlap of the 01:00 one). Read D+1 files up to this HHMM.
next_date = (datetime.strptime(process_date, "%Y%m%d") + timedelta(days=1)).strftime("%Y%m%d")
NEXT_DAY_MAX_HHMM = optional_arg("NEXT_DAY_MAX_HHMM", "0200")

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Overwrite only the partitions in this batch; don't wipe other days/metrics/clusters
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
# Day assignment from `time` must be in UTC, like the fetch's dt folders
spark.conf.set("spark.sql.session.timeZone", "UTC")
# Keep dt a "YYYYMMDD" string instead of letting partition discovery make it an int
spark.conf.set("spark.sql.sources.partitionColumnTypeInference.enabled", "false")
# Write `time` as UTC INT64 micros like the conformed input, not Spark's default INT96
# (deprecated, no row-group stats for time filters, read back as naive nanoseconds)
spark.conf.set("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")

source_base = args["SOURCE_BASE_PATH"].rstrip("/")
output_base = args["OUTPUT_BASE_PATH"].rstrip("/")

# --- Fault-tolerance settings (all optional job args) -----------------------
# Max fraction of files allowed to be skipped before the job fails on purpose.
MAX_DRIFT_RATIO = float(optional_arg("MAX_DRIFT_RATIO", "0.2"))
# Where the per-file drift report goes.
DRIFT_REPORT_BASE = optional_arg(
   "DRIFT_REPORT_PATH", f"{output_base}/_schema_drift"
).rstrip("/")
# Optional pinned schema, e.g. '{"value": "double", "count": "int64"}'
# (Arrow type names). If omitted, the majority type per column wins.
# Generate it with scripts/glue_expected_schema.py. When pinned, the output keeps only these columns.
EXPECTED_SCHEMA_JSON = optional_arg("EXPECTED_SCHEMA_JSON", "")
# A day with no input files fails the job unless this is "true".
ALLOW_EMPTY = optional_arg("ALLOW_EMPTY", "false").lower() == "true"

conformed_root = f"{source_base}/conformed"
# dt= is the LEAF level: conformed/metric=*/cluster=*/dt=YYYYMMDD/HHMM.parquet
glob_path = f"{conformed_root}/metric=*/cluster=*/dt={{{process_date},{next_date}}}"
print(f"Scanning conformed parquet under {glob_path}")


# --- 1. List files with the same glob semantics Spark uses -----------------
def list_parquet_files(pattern_str):
   jvm = sc._jvm
   conf = sc._jsc.hadoopConfiguration()
   pattern = jvm.org.apache.hadoop.fs.Path(pattern_str)
   fs = pattern.getFileSystem(conf)
   dirs = fs.globStatus(pattern) or []
   found = []
   for d in dirs:
       it = fs.listFiles(d.getPath(), True)
       while it.hasNext():
           st = it.next()
           name = st.getPath().getName()
           if name.startswith(("_", ".")):  # _SUCCESS, temp files, etc.
               continue
           found.append((st.getPath().toString(), int(st.getLen())))
   return found


# .../dt=YYYYMMDD/HHMM.parquet -> fetch end time, used to prefer the later fetch in overlaps
FETCH_RE = re.compile(r"/dt=(\d{8})/(\d{4})[^/]*$")


def wanted(path):
   """All of dt=D; from dt=D+1 only the early fetches that can still hold day D rows."""
   if f"/dt={next_date}/" not in path:
       return True
   m = FETCH_RE.search(path)
   return bool(m) and m.group(2) <= NEXT_DAY_MAX_HHMM


files = [f for f in list_parquet_files(glob_path) if wanted(f[0])]
day_files = sum(f"/dt={process_date}/" in p for p, _ in files)
print(f"Found {day_files} parquet files in dt={process_date} and "
     f"{len(files) - day_files} in dt={next_date} up to {NEXT_DAY_MAX_HHMM}")

if not day_files:
   msg = f"No conformed files found for dt={process_date} under {glob_path}"
   if not ALLOW_EMPTY:
       # A missing day means the fetch (or the paths) broke; don't let it pass as green.
       raise RuntimeError(f"{msg}. Set ALLOW_EMPTY=true to exit cleanly instead.")
   print(f"{msg}. Exiting without writing.")
   job.commit()
   sys.exit(0)
if day_files == len(files):
   print(f"  WARNING: nothing from dt={next_date} yet, so the last hour of {process_date} "
         f"will be missing. Rerun after the 00:00 fetch has landed.")


# --- 2. Read only the footers, in parallel on the executors ----------------
def _split_s3(path):
   m = re.match(r"^s3[an]?://([^/]+)/(.+)$", path)
   if not m:
       raise ValueError(f"Not an S3 path: {path}")
   return m.group(1), m.group(2)


def normalize_type(t):
   """Collapse Arrow types that Spark treats as identical."""
   t = t.replace("large_string", "string").replace("large_binary", "binary")
   m = re.match(r"dictionary<values=([^,]+),", t)
   return m.group(1) if m else t


def inspect_partition(rows):
   import boto3
   import pyarrow as pa
   import pyarrow.parquet as pq
   from botocore.config import Config

   s3 = boto3.client(
       "s3", config=Config(retries={"max_attempts": 10, "mode": "adaptive"})
   )
   for path, size in rows:
       try:
           if size < 12:
               raise ValueError(f"file too small to be parquet ({size} bytes)")
           bucket, key = _split_s3(path)
           # Last 8 bytes = 4-byte footer length + "PAR1" magic
           tail8 = s3.get_object(Bucket=bucket, Key=key, Range="bytes=-8")[
               "Body"
           ].read()
           if len(tail8) != 8 or tail8[4:] != b"PAR1":
               raise ValueError("missing PAR1 magic (truncated or not parquet)")
           footer_len = struct.unpack("<I", tail8[:4])[0]
           if footer_len + 12 > size:
               raise ValueError("footer length larger than file")
           tail = s3.get_object(
               Bucket=bucket, Key=key, Range=f"bytes=-{footer_len + 8}"
           )["Body"].read()
           # Rebuild a minimal parquet buffer: magic + footer + len + magic
           meta = pq.read_metadata(pa.BufferReader(b"PAR1" + tail))
           schema = meta.schema.to_arrow_schema()
           cols = {f.name: normalize_type(str(f.type)) for f in schema}
           yield (path, cols, meta.num_rows, None)
       except Exception as e:
           yield (path, None, None, f"{type(e).__name__}: {e}")


num_slices = max(1, min(len(files), sc.defaultParallelism * 2))
inspected = (
   sc.parallelize(files, num_slices).mapPartitions(inspect_partition).collect()
)


# --- 3. Decide the canonical type per column --------------------------------
if EXPECTED_SCHEMA_JSON:
   canonical = {
       c: normalize_type(t) for c, t in json.loads(EXPECTED_SCHEMA_JSON).items()
   }
   print(f"Using pinned schema for {len(canonical)} columns")
else:
   votes = defaultdict(Counter)
   for _, cols, _, err in inspected:
       if cols:
           for c, t in cols.items():
               votes[c][t] += 1
   canonical = {}
   for c, counter in votes.items():
       ranked = counter.most_common()
       canonical[c] = ranked[0][0]
       if len(ranked) > 1:
           print(f"Column '{c}' has multiple types across files: {dict(counter)}")
           if ranked[0][1] == ranked[1][1]:
               print(f"  WARNING: tie for '{c}', picked '{ranked[0][0]}'. "
                     f"Consider pinning EXPECTED_SCHEMA_JSON.")


# --- 4. Classify each file --------------------------------------------------
PART_RE = re.compile(r"metric=([^/]+)/cluster=([^/]+)/")


def issue(path, issue_type, action, column=None, found=None, expected=None, detail=None):
   m = PART_RE.search(path)
   return {
       "dt": process_date,
       "metric": m.group(1) if m else None,
       "cluster": m.group(2) if m else None,
       "file_path": path,
       "issue_type": issue_type,
       "action": action,
       "column": column,
       "found_type": found,
       "expected_type": expected,
       "detail": detail,
   }


good_files, issues = [], []
for path, cols, _, err in inspected:
   if err:
       issues.append(issue(path, "UNREADABLE", "SKIPPED", detail=err))
       continue
   conflicts = [
       (c, t, canonical[c])
       for c, t in cols.items()
       if c in canonical and t != canonical[c]
   ]
   if conflicts:
       for c, t, exp in conflicts:
           issues.append(issue(path, "TYPE_CONFLICT", "SKIPPED", c, t, exp))
       continue
   if EXPECTED_SCHEMA_JSON:
       # The file is kept either way; the output only carries the pinned columns.
       for c in cols:
           if c not in canonical:
               issues.append(issue(path, "NEW_COLUMN", "COLUMN_DROPPED", c, cols[c]))
       for c in canonical:
           if c not in cols:
               issues.append(issue(path, "MISSING_COLUMN", "KEPT", c, None, canonical[c]))
   good_files.append(path)

skipped = len(files) - len(good_files)
print(f"Schema check: {len(good_files)} good, {skipped} skipped of {len(files)}")


# --- 5. Write the drift report and log a summary ----------------------------
if issues:
   by_metric = Counter(i["metric"] for i in issues if i["action"] == "SKIPPED")
   by_column = Counter(
       (i["column"], i["found_type"], i["expected_type"])
       for i in issues
       if i["issue_type"] == "TYPE_CONFLICT"
   )
   print(f"Skipped files by metric: {dict(by_metric)}")
   for (c, f, e), n in by_column.most_common():
       print(f"  {n} file(s): column '{c}' is {f}, expected {e}")

   report_schema = StructType(
       [StructField(k, StringType(), True) for k in issues[0].keys()]
   )
   report_path = f"{DRIFT_REPORT_BASE}/dt={process_date}/"
   (
       spark.createDataFrame(issues, schema=report_schema)
       .coalesce(1)
       .write.mode("overwrite")
       .json(report_path)
   )
   print(f"Wrote drift report to {report_path}")

drift_ratio = skipped / len(files)
if drift_ratio > MAX_DRIFT_RATIO:
   # Fail on purpose: silently dropping a large share of a day is worse than a red run.
   raise RuntimeError(
       f"{skipped}/{len(files)} files ({drift_ratio:.0%}) failed the schema check, "
       f"above MAX_DRIFT_RATIO={MAX_DRIFT_RATIO:.0%}. See {DRIFT_REPORT_BASE}/dt={process_date}/"
   )

if not good_files:
   print("No schema-clean files for this date. Exiting without writing.")
   job.commit()
   sys.exit(0)


# --- 6. Read only the clean files (basePath still recovers partition cols) --
def col(name):
   # Label columns are named "metric.<label>"; unquoted, Spark reads that as field <label>
   # of the `metric` partition column.
   return F.col(f"`{name}`")


df = (
   spark.read.option("basePath", conformed_root)
   .option("mergeSchema", "true")
   .parquet(*good_files)
   # Fetch end time from the file path (YYYYMMDDHHMM), so overlaps can prefer the later fetch
   .withColumn(
       "_fetch",
       F.concat(
           F.regexp_extract(F.input_file_name(), FETCH_RE.pattern, 1),
           F.regexp_extract(F.input_file_name(), FETCH_RE.pattern, 2),
       ),
   )
   # Assign rows to the day of their own timestamp, not the fetch's dt folder
   .filter(F.date_format("time", "yyyyMMdd") == process_date)
   .drop("dt")
)

if EXPECTED_SCHEMA_JSON:
   # Pinned: drop unexpected columns (reported as NEW_COLUMN above)
   df = df.select(*[col(c) for c in canonical if c in df.columns], "metric", "cluster", "_fetch")

data_cols = [c for c in df.columns if c != "_fetch"]
key_cols = [c for c in data_cols if c != "value"]


# --- 7. Drop the overlap between consecutive fetches ------------------------
# Hourly fetches overlap by 5 minutes. A row is one series (every label) at one timestamp;
# when several fetches returned it, keep the value from the latest fetch, which had the
# most time for Sysdig to finish ingesting it.
deduped = (
   df.groupBy(*[col(c) for c in key_cols])
   .agg(
       F.expr("max_by(value, _fetch)").alias("value"),
       F.count(F.lit(1)).alias("_copies"),
       (F.min("value") != F.max("value")).alias("_conflict"),
   )
   .persist()
)

summary = (
   deduped.groupBy("metric", "cluster")
   .agg(
       F.sum("_copies").alias("rows_in"),
       F.count(F.lit(1)).alias("rows_out"),
       F.sum(F.col("_conflict").cast("int")).alias("conflicting"),
   )
   .collect()
)
rows_in = sum(r["rows_in"] for r in summary)
rows_out = sum(r["rows_out"] for r in summary)
print(f"Rows for dt={process_date}: {rows_in} read, {rows_out} after dropping "
     f"{rows_in - rows_out} overlap duplicates")

if rows_out == 0:
   print("No conformed rows found for this date. Exiting without writing.")
   job.commit()
   sys.exit(0)

by_metric = defaultdict(lambda: [0, 0, 0])
for r in summary:
   totals = by_metric[r["metric"]]
   totals[0] += r["rows_in"]
   totals[1] += r["rows_out"]
   totals[2] += r["conflicting"] or 0
for metric, (n_in, n_out, n_conflict) in sorted(by_metric.items()):
   print(f"  {metric}: {n_in} -> {n_out} rows, {n_conflict} kept from the later fetch "
         f"because the copies' values differed")
# Differing copies mean the earlier fetch saw incomplete data (Sysdig ingestion lag)
for r in sorted(summary, key=lambda r: -(r["conflicting"] or 0))[:20]:
   if r["conflicting"]:
       print(f"  differing copies: {r['metric']} / {r['cluster']}: {r['conflicting']}")

output_path = f"{output_base}/conformed_daily/"
(
   deduped.select(*[col(c) for c in data_cols], F.lit(process_date).alias("dt"))
   .repartition(
       "metric", "cluster", "dt"
   )  # co-locate each partition's rows -> no giant per-task sort/spill
   .write.mode("overwrite")
   .partitionBy("metric", "cluster", "dt")
   .option("compression", "snappy")
   .parquet(output_path)
)
deduped.unpersist()

print(f"Wrote combined conformed data to {output_path}")
job.commit()