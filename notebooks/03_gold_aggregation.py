# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer — Life Insurance ETL
# MAGIC
# MAGIC **Goal:** Build a business-ready star schema + aggregates + fraud-flag view on top of Silver.
# MAGIC
# MAGIC **Concepts demonstrated:**
# MAGIC - Star schema design (dimension tables with surrogate keys + fact tables)
# MAGIC - Business aggregates (monthly premium collection, lapse rate by region)
# MAGIC - Rule-based fraud/anomaly flagging (velocity check + amount-exceeds-cover check) —
# MAGIC   conceptually similar to the velocity-based fraud logic from the streaming project,
# MAGIC   applied here in a batch context
# MAGIC - `OPTIMIZE` / `ZORDER` for query performance on large fact tables

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.window import Window

SILVER_DB = "life_insurance_silver"
GOLD_DB = "life_insurance_gold"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {GOLD_DB}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_customer — current-version customers only, with a surrogate key
# MAGIC Gold typically exposes only the *current* view for standard reporting; full history stays
# MAGIC queryable in Silver's SCD2 table for anyone who needs point-in-time analysis.

# COMMAND ----------

dim_customer = (
    spark.table(f"{SILVER_DB}.silver_customers_scd2")
    .filter(F.col("is_current") == True)
    .withColumn("customer_sk", F.monotonically_increasing_id())
    .select("customer_sk", "customer_id", "full_name", "dob", "gender",
            "email", "phone", "city", "state", "pincode")
)

dim_customer.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.dim_customer")
print(f"dim_customer: {dim_customer.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_agent

# COMMAND ----------

dim_agent = (
    spark.table(f"{SILVER_DB}.silver_agents")
    .withColumn("agent_sk", F.monotonically_increasing_id())
    .select("agent_sk", "agent_id", "agent_name", "region", "join_date", "status")
)

dim_agent.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.dim_agent")
print(f"dim_agent: {dim_agent.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_policy — policy attributes + FKs to customer/agent surrogate keys

# COMMAND ----------

policies = spark.table(f"{SILVER_DB}.silver_policies")

dim_policy = (
    policies
    .join(dim_customer.select("customer_id", "customer_sk"), "customer_id", "left")
    .join(dim_agent.select("agent_id", "agent_sk"), "agent_id", "left")
    .withColumn("policy_sk", F.monotonically_increasing_id())
    .select("policy_sk", "policy_id", "customer_sk", "agent_sk", "policy_type",
            "sum_assured", "premium_amount", "premium_frequency",
            "start_date", "end_date", "status")
)

dim_policy.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.dim_policy")
print(f"dim_policy: {dim_policy.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. fact_premium_payments

# COMMAND ----------

payments = spark.table(f"{SILVER_DB}.silver_premium_payments")

fact_premium_payments = (
    payments
    .join(dim_policy.select("policy_id", "policy_sk"), "policy_id", "left")
    .withColumn("payment_year_month", F.date_format("payment_date", "yyyy-MM"))
    .select("payment_id", "policy_sk", "payment_date", "payment_year_month",
            "amount_paid", "late_fee", "payment_mode")
)

fact_premium_payments.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.fact_premium_payments")
print(f"fact_premium_payments: {fact_premium_payments.count()} rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. fact_claims — with rule-based fraud/anomaly flagging
# MAGIC
# MAGIC Two independent fraud signals, combined into one `fraud_flag`:
# MAGIC 1. **Velocity check** — more than one claim on the same policy within a 30-day window
# MAGIC 2. **Amount check** — claim amount exceeds the policy's sum assured (should never happen legitimately)

# COMMAND ----------

claims = spark.table(f"{SILVER_DB}.silver_claims")

claims_with_policy = (
    claims
    .join(dim_policy.select("policy_id", "policy_sk", "sum_assured"), "policy_id", "left")
)

# Velocity check: for each policy, look at the gap to the previous claim on that same policy
policy_window = Window.partitionBy("policy_id").orderBy("claim_date")

claims_velocity_checked = (
    claims_with_policy
    .withColumn("prev_claim_date", F.lag("claim_date").over(policy_window))
    .withColumn("days_since_prev_claim", F.datediff("claim_date", "prev_claim_date"))
    .withColumn("velocity_flag", F.when(F.col("days_since_prev_claim") <= 30, True).otherwise(False))
    .withColumn("amount_exceeds_cover_flag", F.col("claim_amount") > F.col("sum_assured"))
    .withColumn("fraud_flag", F.col("velocity_flag") | F.col("amount_exceeds_cover_flag"))
)

fact_claims = claims_velocity_checked.select(
    "claim_id", "policy_sk", "claim_date", "claim_type", "claim_amount",
    "claim_status", "days_since_prev_claim", "velocity_flag",
    "amount_exceeds_cover_flag", "fraud_flag"
)

fact_claims.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.fact_claims")

total_claims = fact_claims.count()
flagged_claims = fact_claims.filter(F.col("fraud_flag") == True).count()
print(f"fact_claims: {total_claims} rows")
print(f"Flagged as potential fraud: {flagged_claims} rows ({round(100*flagged_claims/total_claims, 1)}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Business aggregates
# MAGIC ### 6a. Monthly premium collection

# COMMAND ----------

monthly_premium_collection = (
    fact_premium_payments
    .groupBy("payment_year_month")
    .agg(
        F.sum("amount_paid").alias("total_premium_collected"),
        F.sum("late_fee").alias("total_late_fees"),
        F.count("payment_id").alias("num_payments")
    )
    .orderBy("payment_year_month")
)

monthly_premium_collection.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.agg_monthly_premium_collection")
display(monthly_premium_collection)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6b. Lapse rate by region
# MAGIC Region comes from the selling agent (`dim_agent`) via `dim_policy`.

# COMMAND ----------

lapse_rate_by_region = (
    dim_policy
    .join(dim_agent.select("agent_sk", "region"), "agent_sk", "left")
    .groupBy("region")
    .agg(
        F.count("policy_id").alias("total_policies"),
        F.sum(F.when(F.col("status") == "Lapsed", 1).otherwise(0)).alias("lapsed_policies")
    )
    .withColumn("lapse_rate_pct", F.round(100 * F.col("lapsed_policies") / F.col("total_policies"), 2))
    .orderBy(F.desc("lapse_rate_pct"))
)

lapse_rate_by_region.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.agg_lapse_rate_by_region")
display(lapse_rate_by_region)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 6c. Fraud-flagged claims detail view (for investigators / dashboards)

# COMMAND ----------

fraud_review_view = (
    fact_claims
    .filter(F.col("fraud_flag") == True)
    .join(dim_policy.select("policy_sk", "policy_id", "customer_sk", "sum_assured"), "policy_sk", "left")
    .join(dim_customer.select("customer_sk", "full_name"), "customer_sk", "left")
    .select("claim_id", "policy_id", "full_name", "claim_date", "claim_amount",
            "sum_assured", "velocity_flag", "amount_exceeds_cover_flag", "claim_status")
)

fraud_review_view.write.format("delta").mode("overwrite").saveAsTable(f"{GOLD_DB}.fraud_review_queue")
display(fraud_review_view.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Performance — OPTIMIZE + ZORDER on the larger fact table
# MAGIC Z-ordering co-locates related data physically, speeding up queries that filter on the
# MAGIC z-order columns. Worth mentioning in interviews even on this dataset size, since it's a
# MAGIC standard production practice on large Delta tables.

# COMMAND ----------

spark.sql(f"OPTIMIZE {GOLD_DB}.fact_premium_payments ZORDER BY (policy_sk, payment_date)")
spark.sql(f"OPTIMIZE {GOLD_DB}.fact_claims ZORDER BY (policy_sk, claim_date)")
print("OPTIMIZE + ZORDER complete on fact tables.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Gold layer summary

# COMMAND ----------

gold_summary = spark.createDataFrame([
    ("dim_customer", dim_customer.count()),
    ("dim_agent", dim_agent.count()),
    ("dim_policy", dim_policy.count()),
    ("fact_premium_payments", fact_premium_payments.count()),
    ("fact_claims", fact_claims.count()),
    ("fraud_review_queue", fraud_review_view.count()),
], ["table_name", "row_count"])

display(gold_summary)
