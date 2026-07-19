%md
## DSP Ad-Delivery Pipeline — README

## Approach

Built a bronze → silver → gold medallion pipeline on Databricks Free Edition using PySpark + Delta tables, processing two DSP event files (v1 and v2 schema) plus an advertiser dimension table.


**Bronze**: loaded both event files as-is (via Unity Catalog tables), tagging every row with schema_version (derived from column presence — media_cost/viewability populated implies v2, otherwise v1), source_file, and ingested_at. No reconciliation or cleanup happens here — bronze is a literal, versioned mirror of what the vendor sent, so any downstream decision can always be traced back to the raw input. Used unionByName(allowMissingColumns=True) to safely merge the 11-column v1 schema and 12-column v2 schema into one superset table without misaligning columns.
**Silver**: unified spend (v1) and media_cost (v2) into a single cost_raw column via COALESCE, confirmed by sampling that this was a genuine rename, not two distinct metrics. Normalized event_time to UTC using try_to_timestamp (not to_timestamp, which throws on malformed input like "N/A" instead of returning NULL) — offsets (Z or +05:30) are embedded per-row in the raw string, confirmed correct via spot-checking real UTC conversions by hand. Quarantined rows failing any of three conditions — malformed timestamp, negative cost, missing currency — into silver.quarantine_events with a quarantine_reason column, rather than dropping them. Deduplicated using row_number() over event_id, ordered by ingest_time descending, which handles both exact duplicates and re-emitted corrections with a single window function.
**Gold**: joined the advertiser dimension via a LEFT JOIN (not inner) so unmapped advertisers still have their spend counted, flagged via is_unmapped_advertiser rather than silently dropped. Converted INR → USD at a fixed rate of 1 USD = 83.00 INR (documented here since it is not a live FX rate). Aggregated to daily spend + event count per advertiser.


## Assumptions


spend (v1) and media_cost (v2) represent the same metric, confirmed by comparing value ranges rather than assuming from naming alone.
Timestamp offsets are per-row, not per-file or per-schema-version — verified directly against raw data rather than inferred from file naming.
Fixed FX rate (83 INR/USD); a production system would join against a daily rates table instead.
Rows with unrecoverable issues (bad timestamp, negative cost, missing currency) are quarantined, not dropped — auditable trail favored over silent data loss.


## Trade-offs


Chose a batch, re-runnable pipeline (mode("overwrite") on read/write) over Databricks Auto Loader streaming with checkpoints. Auto Loader is the correct production answer for true incremental file-by-file ingestion, but for this exercise's scale (two files, manual re-runs), batch overwrite is simpler to reason about and verify, at the cost of not being truly incremental — day-8 files would need to either be appended manually or the pipeline re-pointed at a MERGE (see SQL section) rather than a full overwrite.
A small, known discrepancy (~32 of 57,325 rows, <0.06%) remains between the is_unmapped_advertiser flag and the confirmed-zero left-anti-join orphan count. root cause not fully isolated. Documented rather than silently left in, given its immaterial size relative to the analytics questions asked.

## Client message: 

tonight's vendor file is corrupt and the 9am dashboard SLA will be missed. Write the short message you'd send the client the evening before.
 
My Answer would be:
 Tonight's event file from vendor came through corrupted, so we're not able to run tomorrow morning's pipeline on schedule. This means the 9am dashboard refresh will be delayed.
We're working to resolve it now and will send an update by [specific time, e.g. 7am] with either the corrected dashboard or a firm new ETA. If the file isn't fixed in time, we'll load what we can validate and clearly flag any gaps in the numbers rather than show incomplete data as final.

## 2. Code review — what's wrong with the snippet
pythondef load_events(spark, path):
    df = spark.read.csv(path)
    df = df.dropDuplicates()
    df = df.filter(df.spend != None)
    for row in df.collect():
        if row['spend'] < 0:
            df = df.filter(df.event_id != row['event_id'])
    return df.cache()

1. I think first of all there is not schema or options were mentioned in load_events
2. on a 10x larger file, schema inference alone becomes a meaningful, repeated cost every run. Should pass an explicit StructType schema.
3. In PySpark, col != None doesn't behave like Python's is not None. Spark's null-comparison  mean this comparison evaluates to null for every row. the correct one is df.filter(df.spend.isNotNull())
4. Df.collect() inside a loop - on a 10x-larger, hourly feed, this will either crash the driver with an out-of-memory error.
5. This only catches exact full-row duplicates. It does not handle the "re-emitted event_id with a newer ingest_time should win" case at all.


## 3. Scale-up — 10x larger, hourly instead of daily

1. Move off manual batch overwrites onto true incremental ingestion (Auto Loader + streaming with trigger(availableNow=True) or continuous, checkpointed.
2. The current mode("overwrite") approach recomputes silver from scratch each time — fine at low volume, but at hourly cadence and 10x size, this needs to become a proper MERGE-based upsert.
3. At hourly/10x scale, you need automated checks — this is exactly where the run_dq_checks utility stops being a nice-to-have and becomes a required gate wired into the job, failing loudly (and alerting) rather than silently producing a bad dashboard.
4. Reconsider the gold-layer aggregation grain and refresh strategy. Daily aggregates recomputed hourly are wasteful if most of the day's data hasn't changed. I'd move toward incrementally updating only the current day's (or current hour's) aggregate partition, rather than recomputing the full daily rollup on every run.

