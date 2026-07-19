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
