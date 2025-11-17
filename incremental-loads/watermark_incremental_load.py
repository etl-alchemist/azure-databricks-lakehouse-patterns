"""
Watermark-Based Incremental Loading
====================================
Process only new records based on high-water mark timestamp.

Pattern: Track the maximum timestamp processed, query only records beyond that point.
Use Case: Append-heavy workloads with reliable timestamp columns.

Author: Harsha Morram
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, max as spark_max, current_timestamp, lit
from delta.tables import DeltaTable
from datetime import datetime

spark = SparkSession.builder.appName("Watermark Incremental Load").getOrCreate()

# Configuration
SOURCE_PATH = "/mnt/bronze/healthcare/claims"
TARGET_PATH = "/mnt/silver/healthcare/claims_incremental"
WATERMARK_TABLE = "control.watermarks"
PIPELINE_NAME = "claims_incremental_watermark"
WATERMARK_COLUMN = "received_date"  # Column to track in source data


def initialize_watermark_table():
    """
    Create watermark control table if it doesn't exist.
    
    Schema:
    - pipeline_name: Unique identifier for this pipeline
    - last_processed_timestamp: High-water mark value
    - updated_at: When watermark was last updated
    - record_count: Number of records processed in last run
    """
    
    watermark_schema = """
        pipeline_name STRING,
        last_processed_timestamp TIMESTAMP,
        updated_at TIMESTAMP,
        record_count BIGINT
    """
    
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {WATERMARK_TABLE} (
                {watermark_schema}
            )
            USING DELTA
            LOCATION '/mnt/control/watermarks'
        """)
        print(f"Watermark table {WATERMARK_TABLE} initialized")
    except Exception as e:
        print(f"Watermark table already exists or error: {e}")


def get_last_watermark(pipeline_name):
    """
    Retrieve the last processed timestamp for this pipeline.
    Returns None if this is the first run.
    """
    
    try:
        watermark_df = spark.table(WATERMARK_TABLE)
        
        result = watermark_df \
            .filter(col("pipeline_name") == pipeline_name) \
            .select("last_processed_timestamp") \
            .orderBy(col("updated_at").desc()) \
            .limit(1) \
            .collect()
        
        if result:
            last_timestamp = result[0]["last_processed_timestamp"]
            print(f"Last watermark for {pipeline_name}: {last_timestamp}")
            return last_timestamp
        else:
            print(f"No watermark found for {pipeline_name} - first run")
            return None
            
    except Exception as e:
        print(f"Error retrieving watermark: {e}")
        return None


def update_watermark(pipeline_name, new_watermark, record_count):
    """
    Update the watermark after successful processing.
    
    CRITICAL: Only call this AFTER data has been successfully written to target.
    If processing fails, watermark should NOT advance.
    """
    
    watermark_data = [(
        pipeline_name,
        new_watermark,
        current_timestamp(),
        record_count
    )]
    
    watermark_df = spark.createDataFrame(
        watermark_data,
        ["pipeline_name", "last_processed_timestamp", "updated_at", "record_count"]
    )
    
    # Append new watermark entry (keep history for auditing)
    watermark_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(WATERMARK_TABLE)
    
    print(f"Watermark updated: {new_watermark} ({record_count} records)")


def process_incremental_data():
    """
    Main incremental processing logic using watermark pattern.
    """
    
    print(f"Starting incremental load for {PIPELINE_NAME}")
    
    # Step 1: Get last watermark
    last_watermark = get_last_watermark(PIPELINE_NAME)
    
    # Step 2: Read source data
    source_df = spark.read.format("delta").load(SOURCE_PATH)
    
    # Step 3: Filter for new records only
    if last_watermark:
        print(f"Filtering records where {WATERMARK_COLUMN} > {last_watermark}")
        incremental_df = source_df.filter(col(WATERMARK_COLUMN) > last_watermark)
    else:
        print("First run - processing all available data")
        incremental_df = source_df
    
    # Step 4: Check if there's new data
    record_count = incremental_df.count()
    
    if record_count == 0:
        print("No new records to process")
        return {
            "status": "NO_NEW_DATA",
            "records_processed": 0
        }
    
    print(f"Found {record_count} new records to process")
    
    # Step 5: Add processing metadata
    processed_df = incremental_df \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("pipeline_run", lit(PIPELINE_NAME))
    
    # Step 6: Write to target with MERGE (idempotent)
    if DeltaTable.isDeltaTable(spark, TARGET_PATH):
        print("Merging into existing target table...")
        
        target_table = DeltaTable.forPath(spark, TARGET_PATH)
        
        # Assuming claim_id is the business key
        target_table.alias("target").merge(
            processed_df.alias("source"),
            "target.claim_id = source.claim_id"
        ).whenMatchedUpdateAll() \
         .whenNotMatchedInsertAll() \
         .execute()
        
    else:
        print("Creating new target table...")
        processed_df.write \
            .format("delta") \
            .mode("overwrite") \
            .save(TARGET_PATH)
    
    # Step 7: Calculate new watermark (max timestamp from processed data)
    new_watermark = processed_df.agg(spark_max(WATERMARK_COLUMN)).first()[0]
    
    # Step 8: Update watermark ONLY after successful write
    update_watermark(PIPELINE_NAME, new_watermark, record_count)
    
    print(f"Incremental load completed successfully")
    
    return {
        "status": "SUCCESS",
        "records_processed": record_count,
        "old_watermark": last_watermark,
        "new_watermark": new_watermark
    }


def validate_incremental_load():
    """
    Basic validation to ensure incremental load worked correctly.
    """
    
    target_df = spark.read.format("delta").load(TARGET_PATH)
    
    # Check 1: No duplicate claim_ids (business key uniqueness)
    duplicate_count = target_df.groupBy("claim_id").count().filter(col("count") > 1).count()
    
    if duplicate_count > 0:
        print(f"WARNING: Found {duplicate_count} duplicate claim_ids in target")
        return False
    
    # Check 2: Watermark integrity (no records before last watermark in target)
    last_watermark = get_last_watermark(PIPELINE_NAME)
    
    if last_watermark:
        records_before_watermark = target_df.filter(col(WATERMARK_COLUMN) < last_watermark).count()
        
        if records_before_watermark == 0:
            print("? Watermark integrity check passed")
        else:
            print(f"INFO: {records_before_watermark} records before watermark (expected for backfills)")
    
    # Check 3: Target record count
    total_records = target_df.count()
    print(f"Total records in target: {total_records}")
    
    return True


if __name__ == "__main__":
    """
    Execute watermark-based incremental load.
    
    Execution Flow:
    1. Initialize watermark table (first run only)
    2. Get last processed timestamp
    3. Filter source for new records
    4. Process and write to target
    5. Update watermark
    6. Validate results
    """
    
    try:
        # Initialize watermark tracking
        initialize_watermark_table()
        
        # Process incremental data
        result = process_incremental_data()
        
        # Validate
        if result["status"] == "SUCCESS":
            validate_incremental_load()
        
        print(f"\n? Incremental load result: {result}")
        
    except Exception as e:
        print(f"\n? Incremental load failed: {str(e)}")
        # DO NOT update watermark on failure
        raise
    
    finally:
        spark.stop()


"""
PRODUCTION CONSIDERATIONS:

1. Watermark Column Selection:
   - Use source system's last_modified timestamp (most reliable)
   - Fallback to ingestion_timestamp if source doesn't track changes
   - Avoid business dates (can have retroactive updates)

2. Handling Clock Skew:
   - Source systems may have clock drift
   - Consider adding buffer: WHERE timestamp > last_watermark - INTERVAL 5 MINUTES
   - Trade-off: Small amount of duplicate processing vs. missing records

3. Late-Arriving Data:
   - Watermark pattern can miss records that arrive with old timestamps
   - Options:
     a) Add lookback window (process last N days on each run)
     b) Combine with manifest pattern for critical data
     c) Accept that very late data may be missed (document SLA)

4. Backfill Strategy:
   - To backfill historical data, temporarily set watermark to older date
   - Process backfill separately to avoid mixing with incremental
   - Reset watermark to current after backfill completes

5. Monitoring & Alerts:
   - Alert if watermark hasn't advanced in X hours (pipeline stuck)
   - Alert if record_count drops to zero for Y consecutive runs (source issue)
   - Dashboard showing watermark lag (current_time - last_watermark)

6. Performance Optimization:
   - Ensure source table is partitioned by watermark column
   - Use Z-ORDER on watermark column for faster filtering
   - Consider Photon for large scans

7. Testing:
   - Unit test: Mock watermark table and verify filtering logic
   - Integration test: Process same data twice, verify idempotency
   - Load test: Process large date ranges to validate performance

8. Error Handling:
   - If processing fails midway, watermark should NOT advance
   - Implement transaction-like behavior: write target, THEN update watermark
   - Log failures to separate error table for debugging

9. ADF Integration:
   - Pass last_watermark as pipeline parameter from Lookup activity
   - Return new_watermark to ADF for orchestration logic
   - Use Set Variable activity to update watermark in ADF metadata

Example ADF Parameter Mapping:
{
  "last_watermark": "@activity('GetWatermark').output.firstRow.last_timestamp",
  "pipeline_name": "claims_incremental_watermark",
  "source_path": "/mnt/bronze/healthcare/claims"
}
"""