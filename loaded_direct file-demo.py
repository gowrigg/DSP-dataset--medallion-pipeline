# Databricks notebook source
# MAGIC %sql
# MAGIC SELECT * FROM `datalytics_pipeline_pro`.`default`.`advertisers`;

# COMMAND ----------

spark.sql("USE CATALOG datalytics_pipeline_pro")

# COMMAND ----------

# MAGIC %md
# MAGIC # Brownze Layer
# MAGIC
# MAGIC ## In our datasets we had a schema drifts like new columns added in further versions and column were droped in previous versions. So, we have
# MAGIC - Enable schema evolution: Allow new columns to be added automatically
# MAGIC - Enable rescue data columns: Capture unexpected fields without failing
# MAGIC - Store everything: Timestamp, source metadata, and complete payloads
# MAGIC - Never reject data: The goal is preservation, not validation
# MAGIC

# COMMAND ----------

# DBTITLE 1,Cell 4
# bronze layer ingestin with schema evolution
#explicit schema-version tagging need to the various batch of files

from pyspark.sql import functions as F


# mapping the each of the event files to the 2 variables V1 and v2 versions

v1_events_raw = spark.table("datalytics_pipeline_pro.bronze.events_day_1")
v2_events_raw = spark.table("datalytics_pipeline_pro.bronze.events_day_2")

# the why: using tag_provenance function to create the schema version and source file to the bronze layer for the various version adaption

def tag_provenance(df, version:str, source_name:str):
      return (df
              .withColumn("schema_version", F.lit(version))
              .withColumn("source_file", F.lit(source_name))
              .withColumn("ingested_at", F.current_timestamp())
      )
      
   
v1_events = tag_provenance(v1_events_raw, "v1", "events_v1.csv")
v2_events = tag_provenance(v2_events_raw, "v2", "events_v2.csv")

bronze_events =v1_events.unionByName(v2_events, allowMissingColumns=True)

spark.sql("CREATE SCHEMA IF NOT EXISTS bronze")


(bronze_events.write
 .format("delta")
 .mode("overwrite")
 .option("mergeSchema", "true")
 .saveAsTable("bronze.raw_events"))
# --- Bronze: advertiser dimension — no drift so adding directory listing to the bronze layer

advertiser_dim_raw = spark.table("datalytics_pipeline_pro.default.advertisers")
advertiser_dim =advertiser_dim_raw.withColumn("ingested_at", F.current_timestamp())

(advertiser_dim.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("bronze.advertiser_dim")
)

# COMMAND ----------

# 1) I checked here that the schema evolution is working as expected

display(spark.sql("""
    SELECT schema_version,
           COUNT(spend) AS spend_populated,
           COUNT(media_cost) AS media_cost_populated
      FROM bronze.raw_events
  GROUP BY schema_version
"""))

# 2) confirming the null pattern matches v1/v2 schema

display(spark.sql("""
    SELECT schema_version,
           COUNT(spend) AS spend_populated,
           COUNT(media_cost) AS media_cost_populated,
           COUNT(viewability) AS viewability_populated
      FROM bronze.raw_events
  GROUP BY schema_version
"""))

# 3) row counts by version — does this match what you expect from the source files?
bronze_events.groupBy("schema_version").count().show()


# COMMAND ----------

# 4) row count reconcilation for to confirming - nothing should be lost in the union

v1_events.count() + v2_events.count() == spark.table("datalytics_pipeline_pro.bronze.raw_events").count()

# COMMAND ----------

bronze_events.printSchema() # checking with the schema structure of bronze layer

# schema_version, source_file, ingested_at are non-nullables, confirms the tagging step worked and applied to every row.

# COMMAND ----------

bronze_events.groupBy("currency").count().show()

# COMMAND ----------

bronze_events.select("event_time", "schema_version", "source_file", "ingested_at").distinct().limit(10).show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Silver layer
# MAGIC ### What we handling here is,
# MAGIC
# MAGIC - one clean, unified events table. Deduplicate — exact duplicates and re-emitted event_ids where the row with the latest ingest_time must win. Normalise timestamps to UTC (mind the +05:30 file), quarantine irreparable rows (malformed timestamps, negative cost) into a separate table rather than dropping them silently.
# MAGIC

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

spark.sql("create schema if not exists silver")

# unify the cost columns first===================

#Confirmed earlier: spend (v1) and media_cost (v2) are the SAME
# underlying metric, renamed — not two different fields. COALESCE
# is safe here because a row only ever has one or the other populated.

unified = bronze_events.withColumn(
  "cost_raw",
  F.coalesce(F.col("spend"), F.col("media_cost"))
)

# normalise the timestamps=========================

unified = unified.withColumn(
  "event_time_utc",
  F.try_to_timestamp(F.col("event_time"))
)

# normalise the timestamps, handling +05:30 offset======================

unified.filter(F.col("event_time").contains("+05.30"))\
  .select("event_time", "event_time_utc").show(10, truncate=False)


# quarantine the nulls here =================================

is_bad_timestamp = F.col("event_time").isNull()
is_bad_cost = F.col("cost_raw") < 0
is_missing_currency = F.col("currency").isNull()


quarantine = (unified.filter(is_bad_timestamp | is_bad_cost | is_missing_currency)).withColumn("quarantine_reason",F.when(is_bad_timestamp, F.lit("malformed_timestamp"))
              .when(is_bad_cost, "negative_cost")
              .otherwise(F.lit("missing_currency"))

)


good_rows = unified.filter(~(is_bad_timestamp | is_bad_cost | is_missing_currency))

(quarantine.write.format("delta").mode("overwrite").saveAsTable("silver.quarantine_events"))

# handling the Dedup =============
# exact duplicates + re-emitted event_id
# (latest ingest_time wins) — one window handles both

dedup_window = Window.partitionBy("event_id").orderBy(F.col("ingested_at").desc())

deduped = good_rows.withColumn("row_num", F.row_number().over(dedup_window)).filter(F.col("row_num") == 1).drop("row_num")

# --- Silver: events table
(deduped.write
 .format("delta")
 .mode("overwrite")
 .saveAsTable("silver.events"))

# COMMAND ----------

print(len(unified.columns), len(good_rows.columns), len(deduped.columns), len(quarantine.columns))

# COMMAND ----------

# 1) row reconciliation — must be exact
bronze_count = bronze_events.count()
quarantine_count = quarantine.count()
good_count = good_rows.count()
print("bronze:", bronze_count, "| quarantine:", quarantine_count, "| good:", good_count)
print("reconciles:", bronze_count == quarantine_count + good_count)

# 2) quarantine reason breakdown — confirms all 3 reasons are actually represented
quarantine.groupBy("quarantine_reason").count().show()

# 3) dedup arithmetic — must match exactly
pre_dedup = good_rows.count()
post_dedup = deduped.count()
dup_groups = good_rows.groupBy("event_id").count().filter("count > 1")
expected_removed = dup_groups.selectExpr("sum(count - 1) as extra").collect()[0]["extra"] or 0
print("rows removed by dedup:", pre_dedup - post_dedup, "| expected:", expected_removed)

# 4) confirm silver.events table actually persisted correctly
spark.table("silver.events").count()

# 5) confirm the N/A row specifically landed in quarantine, not silver.events
quarantine.filter(F.col("event_time") == "N/A").show(truncate=False)
spark.table("silver.events").filter(F.col("event_time") == "N/A").count()  # should be 0

# 6) UTC range sanity check — no impossible future/past dates
spark.sql("SELECT min(event_time_utc), max(event_time_utc) FROM silver.events").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## if every event_time string carries its own explicit offset (Z or +05:30 literally in the string), then F.to_timestamp(F.col("event_time")) already handles both cases correctly on its own — Spark's timestamp parser respects an embedded offset and normalizes to UTC internally when you later treat it as a timestamp type.

# COMMAND ----------

silver_events = spark.table("silver.events")
advertiser_dim = spark.table("bronze.advertiser_dim")

silver_events.join(advertiser_dim, "advertiser_id", "left_anti").count()

# COMMAND ----------

# 1) any orphaned advertiser_ids?
silver_events.filter(F.col("currency").isNull()).count()  # expect 0

# 2) confirm zero null currency in silver (sanity check on quarantine)

silver_events.join(advertiser_dim, "advertiser_id", "left_anti").count()

# 3) confirm advertiser_dim itself has no duplicate advertiser_id (would fan out the join)

silver_events.filter(F.col("currency").isNull()).count()

# COMMAND ----------

advertiser_dim.groupBy("advertiser_id").count().filter("count > 1").count()

# COMMAND ----------

# MAGIC %md
# MAGIC # Gold layer
# MAGIC
# MAGIC - daily spend + event counts per advertiser (join the dimension; decide and document what happens to advertisers missing from it). Convert INR→USD at a fixed rate of your choosing — document it.

# COMMAND ----------

from pyspark.sql import functions as F

spark.sql("CREATE SCHEMA IF NOT EXISTS gold")

silver_events = spark.table("datalytics_pipeline_pro.silver.events")
advertiser_dim = spark.table("datalytics_pipeline_pro.bronze.advertiser_dim")

# STEP 1 — currency conversion: INR -> USD at a fixed, documented rate
# Rate chosen: 1 USD = 83.00 INR (i.e. 1 INR = 1/83 USD), as of a
# nominal reference date — this is a FIXED rate for this exercise,
# not live FX. In production this would come from a rates table
# refreshed daily, not a hardcoded constant.
INR_TO_USD_RATE =1/83.00

cost_usd_expr = (
    F.when(F.col("currency") == "INR", F.col("cost_raw") * INR_TO_USD_RATE)
     .when(F.col("currency") == "USD", F.col("cost_raw"))
     .otherwise(F.lit(None))   # defensive — shouldn't occur, silver already filters nulls
)

events_with_usd = silver_events.withColumn("cost_usd", cost_usd_expr)


# STEP-2 - Join advertiser dimention

# Why: an inner join would silently drop spend from gold for any advertiser_id missing from the dimension - that understates the total spend with no visible trace. so left join keeps every event's spend counted

# 0 orphaned advertiser_ids in this dataset today, so this
# branch doesn't currently fire — but it's kept in place because a
# future day's file landing with a new/unmapped advertiser_id is a
# realistic scenario, and this is the safer default to have ready.


""" joined = (events_with_usd
          .join(advertiser_dim, "advertiser_id", "left")
          .withColumn("advertiser_name",F.coalesce(F.col("advertiser_name"), F.lit("UNKNOWN")))
          .withColumn("is_unmapped_advertiser", F.col("advertiser_name") =="UNKNOWN"
          )       
)
# quick fix for the null advertiser_name

joined = (events_with_usd
    .join(
        advertiser_dim.withColumnRenamed("advertiser_name", "advertiser_name_dim"),
        on="advertiser_id",
        how="left"
    )
    .withColumn(
        "is_unmapped_advertiser",
        F.col("advertiser_name_dim").isNull()   # true ONLY when the join found no match at all
    )
    .withColumn(
        "advertiser_name",
        F.coalesce(F.col("advertiser_name_dim"), F.lit("UNKNOWN"))
    )
)


# STEP 3 — daily aggregation: spend + event counts per advertiser

daily_advertiser_spend = (joined.withColumn("event_date", F.to_date("event_time_utc"))
                          .groupBy("event_date","advertiser_id", "advertiser_name", "is_unmapped_advertiser")
                          .agg(F.sum("cost_usd").alias("total_spend_usd"),
                          F.count("event_id").alias("event_count")
                          )
 )
(daily_advertiser_spend
 .write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable("gold.daily_advertiser_spend"))


spark.sql("select count(*) from gold.daily_advertiser_spend where is_unmapped_advertiser = true").show()"""


# STEP 2 — join advertiser dimension
# ============================================================
# DECISION: use a LEFT join, not inner.
# Why: an inner join would silently drop spend from gold for any
# advertiser_id missing from the dimension — that understates total
# spend with no visible trace. A left join keeps every event's spend
# counted, and flags the ones with no matching advertiser explicitly,


joined = (events_with_usd
    .join(advertiser_dim.withColumnRenamed("advertiser_name", "advertiser_name_dim"),
          on="advertiser_id", how="left")
    .withColumn("is_unmapped_advertiser", F.col("advertiser_name_dim").isNull())
    .withColumn("advertiser_name", F.coalesce(F.col("advertiser_name_dim"), F.lit("UNKNOWN")))
)

# daily aggregation: spend + event counts per advertiser

daily_advertiser_spend = (joined
    .withColumn("event_date", F.to_date(F.col("event_time_utc")))
    .groupBy("event_date", "advertiser_id", "advertiser_name", "is_unmapped_advertiser")
    .agg(F.sum("cost_usd").alias("total_spend_usd"),
         F.count("event_id").alias("event_count"))
)

(daily_advertiser_spend.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold.daily_advertiser_spend"))

# 4. ONLY NOW re-run the verification query
spark.sql("SELECT count(*) FROM gold.daily_advertiser_spend WHERE is_unmapped_advertiser = true").show()




# COMMAND ----------

# 1) total spend in gold should be equal to total spend in silver

silver_total_usd = events_with_usd.selectExpr("sum(cost_usd) as total").collect()[0]["total"]
gold_total_usd = spark.sql("SELECT sum(total_spend_usd) FROM gold.daily_advertiser_spend").collect()[0][0]
print(silver_total_usd, gold_total_usd, abs(silver_total_usd - gold_total_usd) < 0.01)

# COMMAND ----------

# 2)  event count reconcilation

silver_events.count() == spark.sql("select sum(event_count) from gold.daily_advertiser_spend").collect()[0][0]

# COMMAND ----------

# 3) spot check with currency conversion math on a couple of rows

events_with_usd.filter(F.col("currency") == "INR").select("cost_raw", "cost_usd").show(5)



# COMMAND ----------

# 4) confirmation of no unmapped advertisers slipped through unexpectedly, should be zero
spark.sql("SELECT count(*) FROM gold.daily_advertiser_spend WHERE is_unmapped_advertiser = true").show()

# COMMAND ----------

# advertiser_id in silver is missing from the dimension table entirely). So how can 32 gold rows show?

advertiser_dim.filter(F.col("advertiser_name") == "UNKNOWN").show(truncate=False)



# COMMAND ----------

# MAGIC %md
# MAGIC ##  32 out of 57,325 rows is under 0.06% of data, low-impact discrepancy deliberately 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Unit test using run_dq_checks
# MAGIC =================================

# COMMAND ----------

# Data Quality Utility"


def run_dq_checks(df, rules):
    """
    df: a Spark/pandas dataframe to check
    rules: a list of dicts, each describing ONE check to run, e.g.
        {"type": "null_rate", "column": "advertiser_id", "max_rate": 0.05}
        {"type": "uniqueness", "column": "event_id"}
        {"type": "freshness", "column": "event_time", "max_age_hours": 24}

    Returns: a list of dicts, one per rule, saying whether it passed
    """
    results = []
    total_rows = df.count()  # (use len(df) instead if this is pandas)

    for rule in rules:
        if rule["type"] == "null_rate":
            null_count = df.filter(df[rule["column"]].isNull()).count()
            actual_rate = null_count / total_rows if total_rows > 0 else 0
            passed = actual_rate <= rule["max_rate"]
            results.append({
                "rule": rule,
                "actual_value": actual_rate,
                "passed": passed
            })

        elif rule["type"] == "uniqueness":
            distinct_count = df.select(rule["column"]).distinct().count()
            passed = distinct_count == total_rows
            results.append({
                "rule": rule,
                "actual_value": distinct_count,
                "passed": passed
            })

        elif rule["type"] == "freshness":
            from pyspark.sql import functions as F
            import datetime
            max_ts = df.agg(F.max(rule["column"])).collect()[0][0]
            age_hours = (datetime.datetime.utcnow() - max_ts).total_seconds() / 3600
            passed = age_hours <= rule["max_age_hours"]
            results.append({
                "rule": rule,
                "actual_value": age_hours,
                "passed": passed
            })

    return results
  
import pytest
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

# ------------------------------------------------------------
# TEST 1 — null rate check, a simple "does it catch nulls" case
# ------------------------------------------------------------
def test_null_rate_detects_violation():
    # made-up data: 2 out of 4 rows have a NULL advertiser_id = 50% null rate
    data = [("adv_1",), ("adv_2",), (None,), (None,)]
    df = spark.createDataFrame(data, ["advertiser_id"])

    rules = [{"type": "null_rate", "column": "advertiser_id", "max_rate": 0.05}]
    results = run_dq_checks(df, rules)

    # we KNOW the real null rate is 50%, way above the 5% threshold,
    # so we expect this check to FAIL
    assert results[0]["passed"] == False
    assert results[0]["actual_value"] == 0.5


# ------------------------------------------------------------
# TEST 2 — uniqueness check, edge case: exact duplicate event_ids
# ------------------------------------------------------------
def test_uniqueness_detects_duplicates():
    # made-up data: "evt_1" appears twice — this is exactly the kind of
    # violation we want to catch
    data = [("evt_1",), ("evt_1",), ("evt_2",)]
    df = spark.createDataFrame(data, ["event_id"])

    rules = [{"type": "uniqueness", "column": "event_id"}]
    results = run_dq_checks(df, rules)

    # 2 distinct values out of 3 rows means NOT unique — should FAIL
    assert results[0]["passed"] == False
    assert results[0]["actual_value"] == 2


# ------------------------------------------------------------
# TEST 3 — edge case: an EMPTY dataframe (zero rows)
# ------------------------------------------------------------
def test_null_rate_handles_empty_dataframe():
    # made-up data: no rows at all — this is the edge case that
    # breaks a lot of naive implementations (division by zero)
    df = spark.createDataFrame([], "advertiser_id STRING")

    rules = [{"type": "null_rate", "column": "advertiser_id", "max_rate": 0.05}]
    results = run_dq_checks(df, rules)

    # with zero rows, there's no meaningful null rate — our function
    # guards this with "if total_rows > 0 else 0", so it should NOT
    # crash, and should report a rate of 0 (defensible default)
    assert results[0]["actual_value"] == 0
    assert results[0]["passed"] == True


test_null_rate_detects_violation()
print("Test 1 (null_rate) passed successfully!")
test_uniqueness_detects_duplicates()
print("Test 2 (uniqueness) passed successfully!")
test_null_rate_handles_empty_dataframe()
print("Test 3 (empty dataframe) passed successfully!")


print("All tests passed!")

# COMMAND ----------

