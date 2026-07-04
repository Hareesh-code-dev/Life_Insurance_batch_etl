# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion — Life Insurance ETL
# MAGIC
# MAGIC **Goal:** Ingest raw CSVs (customers, policies, premium_payments, claims, agents) as-is into
# MAGIC Delta Bronze tables. No business transformation here — only:
# MAGIC - Explicit schema enforcement (no schema inference — production hygiene)
# MAGIC - Lineage metadata (`_ingest_ts`, `_source_file`, `_batch_id`)
# MAGIC - Append-only writes (Bronze is a historical record, never overwritten)
# MAGIC
# MAGIC **Concepts demonstrated:** schema-on-read, StructType schemas, lineage tracking,
# MAGIC idempotent/incremental ingestion pattern, Delta table creation, basic row-count validation.

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Setup — paths & config
# MAGIC Upload the 5 CSVs to a Unity Catalog Volume or DBFS path first, e.g.
# MAGIC `/Volumes/main/life_insurance/raw_landing/` and update `RAW_PATH` below.

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType, DateType
)
import uuid

RAW_PATH = "/Volumes/main/life_insurance/raw_landing"   # <-- update to your uploaded path
BRONZE_DB = "life_insurance_bronze"
BRONZE_PATH = "/Volumes/main/life_insurance/bronze"     # <-- update as needed

BATCH_ID = str(uuid.uuid4())   # unique id per pipeline run, useful for auditing/rollback
INGEST_TS = F.current_timestamp()

spark.sql(f"CREATE DATABASE IF NOT EXISTS {BRONZE_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Explicit schemas
# MAGIC Never rely on `inferSchema=True` in production — it's slow (extra read pass) and silently
# MAGIC guesses wrong types on dirty data. We read everything as **StringType** at Bronze
# MAGIC (schema-on-read philosophy: preserve raw fidelity, cast properly in Silver).

# COMMAND ----------

def all_string_schema(cols):
    return StructType([StructField(c, StringType(), True) for c in cols])

agents_schema = all_string_schema(
    ["agent_id", "agent_name", "region", "join_date", "status"]
)

customers_schema = all_string_schema(
    ["customer_id", "full_name", "dob", "gender", "email", "phone",
     "address", "city", "state", "pincode", "created_at", "updated_at"]
)

policies_schema = all_string_schema(
    ["policy_id", "customer_id", "agent_id", "policy_type", "sum_assured",
     "premium_amount", "premium_frequency", "start_date", "end_date", "status"]
)

payments_schema = all_string_schema(
    ["payment_id", "policy_id", "payment_date", "amount_paid", "late_fee", "payment_mode"]
)

claims_schema = all_string_schema(
    ["claim_id", "policy_id", "customer_id", "claim_date", "claim_type",
     "claim_amount", "claim_status"]
)

SOURCES = {
    "agents": agents_schema,
    "customers": customers_schema,
    "policies": policies_schema,
    "premium_payments": payments_schema,
    "claims": claims_schema,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Generic ingestion function
# MAGIC One reusable function for all 5 tables — avoids copy-pasted read/write logic and is easy
# MAGIC to extend to new source files later.

# COMMAND ----------

def ingest_to_bronze(table_name: str, schema: StructType):
    src_file = f"{RAW_PATH}/{table_name}.csv"

    df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")            # never crash the job on a bad row
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .schema(schema)
        .csv(src_file)
    )

    df_with_meta = (
        df
        .withColumn("_ingest_ts", INGEST_TS)
        .withColumn("_source_file", F.lit(src_file))
        .withColumn("_batch_id", F.lit(BATCH_ID))
    )

    row_count = df_with_meta.count()
    print(f"[{table_name}] read {row_count} rows from {src_file}")

    (
        df_with_meta.write
        .format("delta")
        .mode("append")                 # Bronze is append-only / immutable history
        .option("mergeSchema", "true")  # tolerate upstream schema drift (new columns)
        .saveAsTable(f"{BRONZE_DB}.bronze_{table_name}")
    )

    return row_count

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Run ingestion for all tables + capture a simple audit log

# COMMAND ----------

audit_rows = []
for table_name, schema in SOURCES.items():
    count = ingest_to_bronze(table_name, schema)
    audit_rows.append((table_name, count, BATCH_ID))

audit_df = spark.createDataFrame(audit_rows, ["table_name", "row_count", "batch_id"]) \
    .withColumn("ingested_at", INGEST_TS)

(
    audit_df.write
    .format("delta")
    .mode("append")
    .saveAsTable(f"{BRONZE_DB}.bronze_ingestion_audit")
)

display(audit_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Sanity checks
# MAGIC Quick validation before moving to Silver — not full data quality (that's Silver's job),
# MAGIC just "did ingestion actually work".

# COMMAND ----------

for table_name in SOURCES.keys():
    full_table = f"{BRONZE_DB}.bronze_{table_name}"
    cnt = spark.table(full_table).count()
    corrupt_cnt = 0
    if "_corrupt_record" in spark.table(full_table).columns:
        corrupt_cnt = spark.table(full_table).filter(F.col("_corrupt_record").isNotNull()).count()
    print(f"{full_table}: {cnt} rows | corrupt rows: {corrupt_cnt}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Optional — incremental ingestion with Auto Loader
# MAGIC For a more "real production" version, replace the batch `spark.read.csv` above with
# MAGIC Auto Loader (`cloudFiles`) so new files dropped into `RAW_PATH` are picked up
# MAGIC incrementally without reprocessing everything:
# MAGIC
# MAGIC ```python
# MAGIC df = (
# MAGIC     spark.readStream.format("cloudFiles")
# MAGIC     .option("cloudFiles.format", "csv")
# MAGIC     .option("cloudFiles.schemaLocation", f"{BRONZE_PATH}/_schema/{table_name}")
# MAGIC     .schema(schema)
# MAGIC     .load(RAW_PATH)
# MAGIC )
# MAGIC (df.writeStream
# MAGIC    .format("delta")
# MAGIC    .option("checkpointLocation", f"{BRONZE_PATH}/_checkpoints/{table_name}")
# MAGIC    .trigger(availableNow=True)
# MAGIC    .toTable(f"{BRONZE_DB}.bronze_{table_name}")
# MAGIC )
# MAGIC ```
# MAGIC This is worth mentioning in your project README/interview as the "next iteration" —
# MAGIC shows you know the incremental pattern even if the batch version is what you demo.
