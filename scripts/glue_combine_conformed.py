# glue_combine_conformed.py
# Fault-tolerant version: inspects every parquet footer before reading,
# skips files whose column types drift from the canonical schema,
# writes a drift report, and fails loudly if too much data would be dropped.
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

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Overwrite only the partitions in this batch; don't wipe other days/metrics/clusters
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

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
EXPECTED_SCHEMA_JSON = optional_arg("EXPECTED_SCHEMA_JSON", "")

conformed_root = f"{source_base}/conformed"
# dt= is the LEAF level: conformed/metric=*/cluster=*/dt=YYYYMMDD/
glob_path = f"{conformed_root}/metric=*/cluster=*/dt={process_date}"
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


files = list_parquet_files(glob_path)
print(f"Found {len(files)} parquet files for dt={process_date}")

if not files:
   print("No conformed files found for this date. Exiting without writing.")
   job.commit()
   sys.exit(0)


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
       # New columns merge fine (nulls elsewhere); report but keep the file.
       for c in cols:
           if c not in canonical:
               issues.append(issue(path, "NEW_COLUMN", "KEPT", c, cols[c]))
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
df = (
   spark.read.option("basePath", conformed_root)
   .option("mergeSchema", "true")
   .parquet(*good_files)
)

row_count = df.count()
print(f"Combined row count for dt={process_date}: {row_count}")

if row_count == 0:
   print("No conformed rows found for this date. Exiting without writing.")
   job.commit()
   sys.exit(0)

output_path = f"{output_base}/conformed_daily/"
(
   df.repartition(
       "metric", "cluster", "dt"
   )  # co-locate each partition's rows -> no giant per-task sort/spill
   .write.mode("overwrite")
   .partitionBy("metric", "cluster", "dt")
   .option("compression", "snappy")
   .parquet(output_path)
)

print(f"Wrote combined conformed data to {output_path}")
job.commit()