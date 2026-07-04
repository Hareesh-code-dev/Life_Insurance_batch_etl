# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer — Life Insurance ETL
# MAGIC
# MAGIC **Goal:** Clean, conform, and validate Bronze data into trustworthy Silver tables.
# MAGIC
# MAGIC **Concepts demonstrated in this notebook:**
# MAGIC - Type casting (string → proper date/numeric types)
# MAGIC - Deduplication (exact duplicates + logical duplicates)
# MAGIC - Data quality checks with **quarantine** (bad records routed to a separate table, not silently dropped)
# MAGIC - **SCD Type 2** on `customers` (track address/email history using `effective_date`, `end_date`, `is_current`)
# MAGIC - Referential integrity checks (orphaned policies/claims/payments)
# MAGIC - Idempotent writes using Delta `MERGE INTO`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from delta.tables import DeltaTable

BRONZE_DB = "life_insurance_bronze"
SILVER_DB = "life_insurance_silver"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {SILVER_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Agents — light cleaning (dimension table, low complexity)
# MAGIC Trim whitespace, standardize casing, cast `join_date` to a real DateType, drop exact duplicates.

# COMMAND ----------

agents_bronze = spark.table(f"{BRONZE_DB}.bronze_agents")

agents_silver = (
    agents_bronze
    .dropDuplicates(["agent_id"])   # keep latest bronze load per agent_id
    .withColumn("agent_name", F.trim(F.col("agent_name")))
    .withColumn("region", F.trim(F.initcap(F.col("region"))))
    .withColumn("join_date", F.to_date("join_date", "yyyy-MM-dd"))
    .withColumn("status", F.trim(F.col("status")))
    .select("agent_id", "agent_name", "region", "join_date", "status")
)

agents_silver.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.silver_agents")
print(f"silver_agents: {agents_silver.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Customers — cleaning + Slowly Changing Dimension Type 2
# MAGIC
# MAGIC Bronze has duplicate customer_ids representing the *same customer at different points in time*
# MAGIC (address/email changes) plus some true exact-duplicate rows (same everything). We need to:
# MAGIC 1. Drop exact duplicates
# MAGIC 2. Standardize text fields (trim, casing)
# MAGIC 3. Build SCD2 history — one row per (customer_id, effective period), with `is_current` flag

# COMMAND ----------

customers_bronze = spark.table(f"{BRONZE_DB}.bronze_customers")

customers_clean = (
    customers_bronze
    .dropDuplicates(["customer_id", "full_name", "email", "phone", "address",
                      "city", "state", "pincode", "created_at"])  # drop true exact dupes
    .withColumn("full_name", F.trim(F.col("full_name")))
    .withColumn("city", F.trim(F.initcap(F.col("city"))))
    .withColumn("state", F.trim(F.initcap(F.col("state"))))
    .withColumn("created_at", F.to_timestamp("created_at"))
    .withColumn("updated_at", F.to_timestamp("updated_at"))
    .withColumn("dob", F.to_date("dob", "yyyy-MM-dd"))
    # basic null handling — flag missing contact info rather than dropping the customer
    .withColumn("email", F.when(F.col("email").isNull(), F.lit("UNKNOWN")).otherwise(F.col("email")))
    .withColumn("phone", F.when(F.col("phone").isNull(), F.lit("UNKNOWN")).otherwise(F.col("phone")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2a. Build SCD2 structure
# MAGIC For each `customer_id`, order the versions by `created_at`. The `effective_date` of a version
# MAGIC is its `created_at`; the `end_date` is the *next* version's `effective_date` (or NULL if current).

# COMMAND ----------

window_spec = Window.partitionBy("customer_id").orderBy("created_at")

customers_scd2 = (
    customers_clean
    .withColumn("effective_date", F.col("created_at"))
    .withColumn("end_date", F.lead("created_at").over(window_spec))
    .withColumn("is_current", F.when(F.col("end_date").isNull(), True).otherwise(False))
    .withColumn("scd_row_id", F.concat_ws("_", "customer_id", F.col("effective_date").cast("string")))
)

silver_customers_cols = [
    "scd_row_id", "customer_id", "full_name", "dob", "gender", "email", "phone",
    "address", "city", "state", "pincode", "effective_date", "end_date", "is_current"
]
# Safety dedup: guarantees no two source rows share the same scd_row_id before merging.
# This makes the notebook idempotent even if Bronze has re-ingested the same records
# (Bronze is append-only by design, so reruns can duplicate raw history upstream).
customers_scd2_final = (
    customers_scd2
    .select(*silver_customers_cols)
    .dropDuplicates(["scd_row_id"])
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 2b. Merge into Silver using Delta MERGE (idempotent — safe to re-run)

# COMMAND ----------

silver_customers_table = f"{SILVER_DB}.silver_customers_scd2"

if not spark.catalog.tableExists(silver_customers_table):
    customers_scd2_final.write.format("delta").saveAsTable(silver_customers_table)
else:
    target = DeltaTable.forName(spark, silver_customers_table)
    (
        target.alias("t")
        .merge(customers_scd2_final.alias("s"), "t.scd_row_id = s.scd_row_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )

print(f"{silver_customers_table}: {spark.table(silver_customers_table).count()} rows")
print(f"Current-version customers: {spark.table(silver_customers_table).filter('is_current = true').count()}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Policies — data quality checks with quarantine
# MAGIC Rules enforced:
# MAGIC - `sum_assured` must be positive
# MAGIC - `start_date` must be before `end_date`
# MAGIC - `agent_id` must not be null (every policy needs a selling agent on record)
# MAGIC - `customer_id` must exist in Silver customers (referential integrity)
# MAGIC
# MAGIC Records failing any rule go to a **quarantine table** instead of being silently dropped —
# MAGIC this preserves auditability, a key data quality principle.

# COMMAND ----------

policies_bronze = spark.table(f"{BRONZE_DB}.bronze_policies")
valid_customer_ids = spark.table(silver_customers_table).select("customer_id").distinct()

policies_typed = (
    policies_bronze
    .withColumn("sum_assured", F.col("sum_assured").cast("double"))
    .withColumn("premium_amount", F.col("premium_amount").cast("double"))
    .withColumn("start_date", F.to_date("start_date", "yyyy-MM-dd"))
    .withColumn("end_date", F.to_date("end_date", "yyyy-MM-dd"))
    .dropDuplicates(["policy_id"])
)

policies_checked = (
    policies_typed
    .withColumn("dq_fail_reason",
        F.when(F.col("sum_assured") <= 0, F.lit("non_positive_sum_assured"))
         .when(F.col("start_date") >= F.col("end_date"), F.lit("invalid_date_range"))
         .when(F.col("agent_id").isNull(), F.lit("missing_agent_id"))
         .otherwise(F.lit(None))
    )
)

# referential check against customers separately (needs a join)
policies_with_customer_check = (
    policies_checked
    .join(valid_customer_ids.withColumnRenamed("customer_id", "cid_check"),
          policies_checked.customer_id == F.col("cid_check"), "left")
    .withColumn("dq_fail_reason",
        F.when(F.col("cid_check").isNull(), F.lit("orphaned_customer_id"))
         .otherwise(F.col("dq_fail_reason"))
    )
    .drop("cid_check")
)

policies_valid = policies_with_customer_check.filter(F.col("dq_fail_reason").isNull()).drop("dq_fail_reason")
policies_quarantine = policies_with_customer_check.filter(F.col("dq_fail_reason").isNotNull())

policies_valid.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.silver_policies")
policies_quarantine.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.quarantine_policies")

print(f"silver_policies (valid): {policies_valid.count()} rows")
print(f"quarantine_policies (failed DQ): {policies_quarantine.count()} rows")
display(policies_quarantine.groupBy("dq_fail_reason").count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Premium Payments — dedup + quarantine negative amounts

# COMMAND ----------

payments_bronze = spark.table(f"{BRONZE_DB}.bronze_premium_payments")
valid_policy_ids = policies_valid.select("policy_id").distinct()

payments_typed = (
    payments_bronze
    .dropDuplicates(["payment_id"])   # remove exact duplicate payment records
    .withColumn("amount_paid", F.col("amount_paid").cast("double"))
    .withColumn("late_fee", F.col("late_fee").cast("double"))
    .withColumn("payment_date", F.to_date("payment_date", "yyyy-MM-dd"))
)

payments_checked = (
    payments_typed
    .join(valid_policy_ids.withColumnRenamed("policy_id", "pid_check"),
          payments_typed.policy_id == F.col("pid_check"), "left")
    .withColumn("dq_fail_reason",
        F.when(F.col("amount_paid") <= 0, F.lit("non_positive_amount"))
         .when(F.col("pid_check").isNull(), F.lit("orphaned_policy_id"))
         .otherwise(F.lit(None))
    )
    .drop("pid_check")
)

payments_valid = payments_checked.filter(F.col("dq_fail_reason").isNull()).drop("dq_fail_reason")
payments_quarantine = payments_checked.filter(F.col("dq_fail_reason").isNotNull())

payments_valid.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.silver_premium_payments")
payments_quarantine.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.quarantine_premium_payments")

print(f"silver_premium_payments: {payments_valid.count()} rows")
print(f"quarantine_premium_payments: {payments_quarantine.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Claims — type casting + referential integrity
# MAGIC (Fraud-pattern flagging happens later, in Gold — Silver's job is just clean, valid data.)

# COMMAND ----------

claims_bronze = spark.table(f"{BRONZE_DB}.bronze_claims")

claims_typed = (
    claims_bronze
    .dropDuplicates(["claim_id"])
    .withColumn("claim_amount", F.col("claim_amount").cast("double"))
    .withColumn("claim_date", F.to_date("claim_date", "yyyy-MM-dd"))
)

claims_checked = (
    claims_typed
    .join(valid_policy_ids.withColumnRenamed("policy_id", "pid_check"),
          claims_typed.policy_id == F.col("pid_check"), "left")
    .withColumn("dq_fail_reason",
        F.when(F.col("pid_check").isNull(), F.lit("orphaned_policy_id"))
         .when(F.col("claim_amount") <= 0, F.lit("non_positive_claim_amount"))
         .otherwise(F.lit(None))
    )
    .drop("pid_check")
)

claims_valid = claims_checked.filter(F.col("dq_fail_reason").isNull()).drop("dq_fail_reason")
claims_quarantine = claims_checked.filter(F.col("dq_fail_reason").isNotNull())

claims_valid.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.silver_claims")
claims_quarantine.write.format("delta").mode("overwrite").saveAsTable(f"{SILVER_DB}.quarantine_claims")

print(f"silver_claims: {claims_valid.count()} rows")
print(f"quarantine_claims: {claims_quarantine.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Data quality summary — a mini "DQ dashboard" query
# MAGIC Useful to screenshot for your GitHub README / interview walkthrough.

# COMMAND ----------

dq_summary = spark.createDataFrame([
    ("policies", policies_valid.count(), policies_quarantine.count()),
    ("premium_payments", payments_valid.count(), payments_quarantine.count()),
    ("claims", claims_valid.count(), claims_quarantine.count()),
], ["table_name", "valid_rows", "quarantined_rows"])

display(dq_summary)
