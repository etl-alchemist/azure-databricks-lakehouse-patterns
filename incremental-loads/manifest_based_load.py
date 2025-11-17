"""
Manifest-Based Incremental Loading
===================================
Track which source files have been processed to enable idempotent, replay-capable loads.

Pattern: Maintain a manifest (log) of processed files, only load new files.
Use Case: File-based sources (EDI, CSV exports, Parquet dumps) with replay requirements.

Author: Harsha Morram
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, input_file_name, current_timestamp, lit, 
    count, sum as spark_sum, md5, concat_ws
)
from delta.tables import DeltaTable
from datetime import datetime
import os

spark = SparkSession.builder.appName("Manifest-Based Incremental Load").getOrCreate()

# Configuration
SOURCE_PATH = "/mnt/landing/claims/raw/*.csv"  # Wildcard pattern for source files
TARGET_PATH = "/mnt/silver/healthcare/claims_manifest"
MANIFEST_TABLE = "control.file_manifest"
PIPELINE_NAME = "claims_manifest_based"


def initialize_manifest_table():
    """
    Create manifest table to track processed files.
    
    Schema:
    - pipeline_name: Which pipeline processed this file
    - file_name: Full path or unique identifier of source file
    - file_hash: MD5 checksum to detect file changes
    - processed_timestamp: When file was successfully loaded
    - status: SUCCESS, FAILED, IN_PROGRESS, SUPERSEDED
    - record_count: Number of records in the file
    - file_size_bytes: Size of source file
    - processing_duration_seconds: How long it took to process
    """
    
    try:
        spark.sql(f"""
            CREATE TABLE IF NOT EXISTS {MANIFEST_TABLE} (
                pipeline_name STRING,
                file_name STRING,
                file_hash STRING,
                processed_timestamp TIMESTAMP,
                status STRING,
                record_count BIGINT,
                file_size_bytes BIGINT,
                processing_duration_seconds DOUBLE
            )
            USING DELTA
            LOCATION '/mnt/control/file_manifest'
        """)
        print(f"Manifest table {MANIFEST_TABLE} initialized")
    except Exception as e:
        print(f"Manifest table already exists or error: {e}")


def get_processed_files(pipeline_name):
    """
    Retrieve list of files that have already been successfully processed.
    """
    
    try:
        manifest_df = spark.table(MANIFEST_TABLE)
        
        processed_files = manifest_df \
            .filter(
                (col("pipeline_name") == pipeline_name) &
                (col("status") == "SUCCESS")
            ) \
            .select("file_name") \
            .rdd.flatMap(lambda x: x).collect()
        
        print(f"Found {len(processed_files)} already processed files")
        return set(processed_files)
        
    except Exception as e:
        print(f"Error retrieving manifest: {e}")
        return set()


def get_source_files():
    """
    List all available source files matching the pattern.
    """
    
    # Get list of files matching pattern
    try:
        # Read one record to get file list
        sample_df = spark.read \
            .option("header", "true") \
            .csv(SOURCE_PATH) \
            .withColumn("source_file", input_file_name()) \
            .select("source_file") \
            .distinct()
        
        source_files = sample_df.rdd.flatMap(lambda x: x).collect()
        print(f"Found {len(source_files)} source files")
        
        return source_files
        
    except Exception as e:
        print(f"Error listing source files: {e}")
        return []


def calculate_file_hash(file_path):
    """
    Calculate MD5 hash of file content to detect changes.
    In production, this would read actual file bytes.
    """
    # Simplified: use file name + size as proxy for hash
    # In production: use dbutils.fs.head() or read file bytes
    return md5(lit(file_path)).alias("file_hash")


def process_file(file_path, pipeline_name):
    """
    Process a single source file and track in manifest.
    """
    
    start_time = datetime.now()
    
    print(f"Processing file: {file_path}")
    
    try:
        # Read the specific file
        file_df = spark.read \
            .option("header", "true") \
            .option("inferSchema", "true") \
            .csv(file_path)
        
        record_count = file_df.count()
        
        # Add metadata columns
        processed_df = file_df \
            .withColumn("source_file_name", lit(file_path)) \
            .withColumn("processed_timestamp", current_timestamp()) \
            .withColumn("pipeline_name", lit(pipeline_name))
        
        # Write to target (append mode for file-based processing)
        if DeltaTable.isDeltaTable(spark, TARGET_PATH):
            # Merge to handle potential file reprocessing
            target_table = DeltaTable.forPath(spark, TARGET_PATH)
            
            target_table.alias("target").merge(
                processed_df.alias("source"),
                """
                target.claim_id = source.claim_id AND
                target.source_file_name = source.source_file_name
                """
            ).whenMatchedUpdateAll() \
             .whenNotMatchedInsertAll() \
             .execute()
        else:
            processed_df.write \
                .format("delta") \
                .mode("overwrite") \
                .save(TARGET_PATH)
        
        # Calculate processing duration
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        # Update manifest with success
        update_manifest(
            pipeline_name=pipeline_name,
            file_name=file_path,
            status="SUCCESS",
            record_count=record_count,
            duration=duration
        )
        
        print(f"? File processed successfully: {record_count} records in {duration:.2f}s")
        
        return {
            "status": "SUCCESS",
            "file": file_path,
            "records": record_count,
            "duration": duration
        }
        
    except Exception as e:
        # Log failure to manifest
        update_manifest(
            pipeline_name=pipeline_name,
            file_name=file_path,
            status="FAILED",
            record_count=0,
            duration=0
        )
        
        print(f"? File processing failed: {str(e)}")
        
        return {
            "status": "FAILED",
            "file": file_path,
            "error": str(e)
        }


def update_manifest(pipeline_name, file_name, status, record_count, duration):
    """
    Update manifest table with file processing result.
    """
    
    manifest_entry = [(
        pipeline_name,
        file_name,
        "placeholder_hash",  # In production: calculate actual MD5
        current_timestamp(),
        status,
        record_count,
        0,  # file_size_bytes - would calculate from dbutils.fs.ls()
        duration
    )]
    
    manifest_df = spark.createDataFrame(
        manifest_entry,
        [
            "pipeline_name", "file_name", "file_hash", "processed_timestamp",
            "status", "record_count", "file_size_bytes", "processing_duration_seconds"
        ]
    )
    
    manifest_df.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(MANIFEST_TABLE)


def process_incremental_files():
    """
    Main orchestration: identify new files and process them.
    """
    
    print(f"Starting manifest-based incremental load for {PIPELINE_NAME}")
    
    # Step 1: Get list of already processed files
    processed_files = get_processed_files(PIPELINE_NAME)
    
    # Step 2: Get list of available source files
    source_files = get_source_files()
    
    # Step 3: Identify new files (anti-join)
    new_files = [f for f in source_files if f not in processed_files]
    
    if not new_files:
        print("No new files to process")
        return {
            "status": "NO_NEW_FILES",
            "files_processed": 0
        }
    
    print(f"Found {len(new_files)} new files to process")
    
    # Step 4: Process each new file
    results = []
    total_records = 0
    
    for file_path in new_files:
        result = process_file(file_path, PIPELINE_NAME)
        results.append(result)
        
        if result["status"] == "SUCCESS":
            total_records += result["records"]
    
    # Step 5: Summary
    successful = len([r for r in results if r["status"] == "SUCCESS"])
    failed = len([r for r in results if r["status"] == "FAILED"])
    
    print(f"\nProcessing complete:")
    print(f"  - Files processed: {successful}")
    print(f"  - Files failed: {failed}")
    print(f"  - Total records: {total_records}")
    
    return {
        "status": "COMPLETE",
        "files_processed": successful,
        "files_failed": failed,
        "total_records": total_records,
        "results": results
    }


def replay_specific_file(file_name):
    """
    Replay/reprocess a specific file (e.g., corrected data).
    
    This marks the old entry as SUPERSEDED and reprocesses the file.
    """
    
    print(f"Replaying file: {file_name}")
    
    # Mark existing entry as superseded
    spark.sql(f"""
        UPDATE {MANIFEST_TABLE}
        SET status = 'SUPERSEDED'
        WHERE file_name = '{file_name}'
        AND pipeline_name = '{PIPELINE_NAME}'
        AND status = 'SUCCESS'
    """)
    
    # Reprocess the file
    result = process_file(file_name, PIPELINE_NAME)
    
    print(f"Replay complete: {result}")
    return result


def get_manifest_summary():
    """
    Generate summary statistics from manifest for monitoring.
    """
    
    manifest_df = spark.table(MANIFEST_TABLE)
    
    summary = manifest_df \
        .filter(col("pipeline_name") == PIPELINE_NAME) \
        .groupBy("status") \
        .agg(
            count("*").alias("file_count"),
            spark_sum("record_count").alias("total_records")
        )
    
    summary.show()
    
    return summary


if __name__ == "__main__":
    """
    Execute manifest-based incremental load.
    
    Execution Flow:
    1. Initialize manifest table
    2. Get list of processed files from manifest
    3. Get list of available source files
    4. Process new files only
    5. Update manifest for each file
    """
    
    try:
        # Initialize manifest tracking
        initialize_manifest_table()
        
        # Process new files
        result = process_incremental_files()
        
        # Show manifest summary
        get_manifest_summary()
        
        print(f"\n? Manifest-based incremental load result: {result}")
        
    except Exception as e:
        print(f"\n? Manifest-based load failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


"""
PRODUCTION CONSIDERATIONS:

1. File Naming Conventions:
   - Establish clear pattern: claims_YYYYMMDD_HHmmss.csv
   - Include date/timestamp in filename for easy identification
   - Avoid generic names like "data.csv" that get overwritten

2. Handling File Corrections:
   - Use replay_specific_file() to reprocess corrected data
   - Mark original as SUPERSEDED, not deleted (audit trail)
   - Consider versioning: claims_20241101_v1.csv, claims_20241101_v2.csv

3. Idempotency:
   - MERGE operations ensure reprocessing same file doesn't create duplicates
   - Use composite key: (claim_id, source_file_name) for deduplication
   - Safe to re-run failed files without data corruption

4. Large File Handling:
   - For files > 1GB, consider processing in chunks
   - Use Spark's file splitting capabilities
   - Track chunks separately in manifest (file_name + chunk_id)

5. File Archival:
   - After successful processing, move files to archive location
   - Keep manifest entry pointing to archive path
   - Implement retention policy (delete files > 90 days)

6. Late-Arriving Files:
   - Manifest pattern handles this naturally
   - File from last week arrives today? Gets processed automatically
   - No watermark advancement issues

7. Failed File Handling:
   - Don't block entire pipeline on one bad file
   - Mark as FAILED, continue with other files
   - Set up alert for FAILED status in manifest
   - Implement retry logic with exponential backoff

8. Monitoring:
   - Dashboard showing:
     * Files processed per day
     * Average processing time per file
     * Files in FAILED status
     * Oldest unprocessed file (data freshness lag)
   
9. Testing:
   - Unit test: Mock file list and manifest
   - Integration test: Process same file twice, verify idempotency
   - Chaos test: Simulate failures midway through batch

10. ADF Integration:
    - Use Get Metadata activity to list source files
    - ForEach activity to process files in parallel (with concurrency limit)
    - Store manifest in SQL Database for ADF Lookup activities

Example ADF Pipeline:
+--------------------------------------------------+
¦ Get Metadata: List Files                         ¦
¦   +-> ForEach: file in files                     ¦
¦       +-> Lookup: Check if processed             ¦
¦           +-> If: Not processed                  ¦
¦               +-> Databricks: process_file()     ¦
¦                   +-> Update Manifest            ¦
+--------------------------------------------------+

11. Performance:
    - Parallelize file processing using Spark DataFrame partitioning
    - Use Photon for faster CSV parsing
    - Consider Auto Loader for continuous file ingestion
    
12. Security:
    - Manifest table contains file paths - ensure proper access controls
    - Don't log PII in manifest (record_count is OK, sample data is not)
    - Encrypt manifest if it contains sensitive metadata
"""