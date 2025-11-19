"""
SCD Type 2 - Member Dimension (Healthcare)
===========================================
Track member attribute changes over time with effective dating.

Use Case: Member plan changes, address updates, PCP assignments, risk scores
Pattern: Close old record, insert new record with updated attributes

Author: Harsha Morram
Based on: BCBS member eligibility tracking (834 EDI feeds)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, to_date, when, coalesce,
    max as spark_max, row_number, monotonically_increasing_id,
    hash, md5, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("SCD Type 2 - Member Dimension").getOrCreate()

# Configuration
SOURCE_PATH = "/mnt/silver/healthcare/member_master"  # Incoming member data
TARGET_DIM_PATH = "/mnt/gold/healthcare/dim_member"
HIGH_DATE = "9999-12-31"  # Sentinel value for current records


def get_max_surrogate_key():
    """
    Get the maximum surrogate key currently in the dimension.
    Used to generate new surrogate keys for new/updated records.
    """
    
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
        max_sk = dim_df.agg(spark_max("member_sk")).first()[0]
        return max_sk if max_sk else 0
    else:
        return 0


def load_source_data():
    """
    Load incoming member data from source system.
    This represents the current state as of today.
    """
    
    # In production, this would come from Silver layer after processing 834 EDI files
    source_df = spark.read.format("delta").load(SOURCE_PATH)
    
    # Select attributes we want to track with SCD2
    tracked_attributes = [
        "member_id",        # Natural key
        "first_name",
        "last_name",
        "date_of_birth",
        "gender",
        "plan_type",        # Track: Gold, Silver, Bronze changes
        "zip_code",         # Track: Address changes
        "pcp_id",           # Track: Primary care physician changes
        "risk_score",       # Track: Risk score changes
        "coverage_start",
        "coverage_end"
    ]
    
    return source_df.select(*tracked_attributes)


def load_current_dimension():
    """
    Load only current (active) records from dimension.
    These are the records we'll compare against to detect changes.
    """
    
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
        
        # Filter for current records only
        current_df = dim_df.filter(col("is_current") == True)
        
        return current_df
    else:
        # First load - no existing dimension
        return None


def identify_changes(source_df, current_dim_df):
    """
    Compare source data with current dimension to identify:
    - New members (INSERT)
    - Changed attributes (SCD2: close old, insert new)
    - No change (do nothing)
    """
    
    if current_dim_df is None:
        # First load - everything is new
        return source_df.withColumn("change_type", lit("NEW"))
    
    # Join source to current dimension on natural key
    comparison_df = source_df.alias("src").join(
        current_dim_df.alias("dim"),
        col("src.member_id") == col("dim.member_id"),
        "left"
    )
    
    # Detect changes in tracked attributes
    # Using MD5 hash for simplicity (could compare each column individually)
    comparison_df = comparison_df \
        .withColumn(
            "src_hash",
            md5(concat_ws("|",
                col("src.plan_type"),
                col("src.zip_code"),
                col("src.pcp_id"),
                col("src.risk_score")
            ))
        ) \
        .withColumn(
            "dim_hash",
            md5(concat_ws("|",
                coalesce(col("dim.plan_type"), lit("")),
                coalesce(col("dim.zip_code"), lit("")),
                coalesce(col("dim.pcp_id"), lit("")),
                coalesce(col("dim.risk_score"), lit(""))
            ))
        )
    
    # Classify each record
    classified_df = comparison_df \
        .withColumn(
            "change_type",
            when(col("dim.member_id").isNull(), "NEW")  # Not in dimension = new member
            .when(col("src_hash") != col("dim_hash"), "CHANGED")  # Hash mismatch = attribute changed
            .otherwise("NO_CHANGE")  # Hash match = no change
        )
    
    # Select source columns + metadata
    result_df = classified_df.select(
        col("src.member_id"),
        col("src.first_name"),
        col("src.last_name"),
        col("src.date_of_birth"),
        col("src.gender"),
        col("src.plan_type"),
        col("src.zip_code"),
        col("src.pcp_id"),
        col("src.risk_score"),
        col("src.coverage_start"),
        col("src.coverage_end"),
        col("change_type"),
        col("dim.member_sk").alias("existing_member_sk")  # Needed to close old record
    )
    
    return result_df


def process_new_members(new_members_df, start_surrogate_key):
    """
    Process new members - simple INSERT with SCD2 metadata.
    """
    
    print(f"Processing {new_members_df.count()} new members...")
    
    # Generate surrogate keys
    window_spec = Window.orderBy("member_id")
    
    new_records = new_members_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("member_sk", col("row_num") + start_surrogate_key) \
        .withColumn("effective_start_date", current_date()) \
        .withColumn("effective_end_date", to_date(lit(HIGH_DATE))) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_timestamp", current_date()) \
        .withColumn("updated_timestamp", current_date()) \
        .drop("row_num", "change_type", "existing_member_sk")
    
    return new_records


def process_changed_members(changed_members_df, start_surrogate_key):
    """
    Process changed members - requires two steps:
    1. Close existing record (UPDATE)
    2. Insert new record (INSERT)
    """
    
    print(f"Processing {changed_members_df.count()} changed members...")
    
    # Generate new surrogate keys for new versions
    window_spec = Window.orderBy("member_id")
    
    new_versions = changed_members_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("member_sk", col("row_num") + start_surrogate_key) \
        .withColumn("effective_start_date", current_date()) \
        .withColumn("effective_end_date", to_date(lit(HIGH_DATE))) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_timestamp", current_date()) \
        .withColumn("updated_timestamp", current_date()) \
        .drop("row_num", "change_type", "existing_member_sk")
    
    # Prepare records to close (old versions)
    # These will be used in MERGE to update existing records
    records_to_close = changed_members_df.select(
        "member_id",
        "existing_member_sk"
    ).withColumn("close_date", current_date())
    
    return new_versions, records_to_close


def apply_scd2_changes(new_records, records_to_close):
    """
    Apply SCD Type 2 changes to dimension table using Delta MERGE.
    
    Steps:
    1. Close old records (set effective_end_date, is_current = False)
    2. Insert new records
    """
    
    dim_table = DeltaTable.forPath(spark, TARGET_DIM_PATH)
    
    # Step 1: Close old records
    if records_to_close and records_to_close.count() > 0:
        print("Closing old member records...")
        
        # Calculate day before effective start of new record
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        # MERGE to update old records
        dim_table.alias("target").merge(
            records_to_close.alias("source"),
            "target.member_sk = source.existing_member_sk"
        ).whenMatchedUpdate(set={
            "effective_end_date": lit(yesterday),
            "is_current": lit(False),
            "updated_timestamp": current_date()
        }).execute()
    
    # Step 2: Insert new records (both new members and new versions)
    if new_records.count() > 0:
        print(f"Inserting {new_records.count()} new dimension records...")
        
        new_records.write \
            .format("delta") \
            .mode("append") \
            .save(TARGET_DIM_PATH)


def process_scd2_member_dimension():
    """
    Main orchestration for SCD Type 2 member dimension processing.
    """
    
    print("Starting SCD Type 2 member dimension processing...")
    
    # Step 1: Load source data (current state)
    source_df = load_source_data()
    print(f"Loaded {source_df.count()} members from source")
    
    # Step 2: Load current dimension records
    current_dim_df = load_current_dimension()
    
    # Step 3: Identify changes
    classified_df = identify_changes(source_df, current_dim_df)
    
    # Step 4: Split by change type
    new_members = classified_df.filter(col("change_type") == "NEW")
    changed_members = classified_df.filter(col("change_type") == "CHANGED")
    no_change = classified_df.filter(col("change_type") == "NO_CHANGE")
    
    print(f"\nChange summary:")
    print(f"  - New members: {new_members.count()}")
    print(f"  - Changed members: {changed_members.count()}")
    print(f"  - No change: {no_change.count()}")
    
    # Step 5: Get starting surrogate key
    current_max_sk = get_max_surrogate_key()
    
    # Step 6: Process new members
    new_records = None
    records_to_close = None
    
    if new_members.count() > 0:
        new_records = process_new_members(new_members, current_max_sk + 1)
        current_max_sk += new_members.count()
    
    # Step 7: Process changed members
    if changed_members.count() > 0:
        new_versions, records_to_close = process_changed_members(
            changed_members, 
            current_max_sk + 1
        )
        
        # Combine new members and new versions
        if new_records:
            new_records = new_records.union(new_versions)
        else:
            new_records = new_versions
    
    # Step 8: Apply changes to dimension
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        apply_scd2_changes(new_records, records_to_close)
    else:
        # First load - create dimension table
        print("Creating dimension table...")
        if new_records:
            new_records.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("is_current") \
                .save(TARGET_DIM_PATH)
    
    # Register table
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_member USING DELTA LOCATION '{TARGET_DIM_PATH}'")
    
    print("\nSCD Type 2 processing completed")
    
    return {
        "new_members": new_members.count() if new_members else 0,
        "changed_members": changed_members.count() if changed_members else 0,
        "no_change": no_change.count() if no_change else 0
    }


def query_member_history(member_id):
    """
    Utility function to view complete history for a member.
    """
    
    dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
    
    history = dim_df \
        .filter(col("member_id") == member_id) \
        .select(
            "member_sk",
            "member_id",
            "plan_type",
            "zip_code",
            "pcp_id",
            "risk_score",
            "effective_start_date",
            "effective_end_date",
            "is_current"
        ) \
        .orderBy("effective_start_date")
    
    print(f"\nHistory for member {member_id}:")
    history.show(truncate=False)
    
    return history


def query_point_in_time(as_of_date):
    """
    Query dimension as of a specific date (point-in-time query).
    """
    
    dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
    
    snapshot = dim_df.filter(
        (col("effective_start_date") <= as_of_date) &
        (col("effective_end_date") >= as_of_date)
    )
    
    print(f"\nMember dimension snapshot as of {as_of_date}:")
    print(f"Total members: {snapshot.count()}")
    
    # Show plan type distribution
    snapshot.groupBy("plan_type").count().show()
    
    return snapshot


if __name__ == "__main__":
    """
    Execute SCD Type 2 member dimension processing.
    """
    
    try:
        # Process dimension
        stats = process_scd2_member_dimension()
        
        print(f"\n? SCD Type 2 processing complete:")
        print(f"  - New members added: {stats['new_members']}")
        print(f"  - Members updated: {stats['changed_members']}")
        print(f"  - Members unchanged: {stats['no_change']}")
        
        # Example queries (uncomment to run)
        # query_member_history("M001")
        # query_point_in_time("2024-01-01")
        
    except Exception as e:
        print(f"\n? SCD Type 2 processing failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


"""
PRODUCTION CONSIDERATIONS:

1. Surrogate Key Generation:
   - Current approach: Sequential (max + 1)
   - Alternative: Hash-based for deterministic keys
   - Consider: UUID for distributed processing

2. Attribute Comparison:
   - Using MD5 hash for simplicity
   - Production: Compare each attribute individually for better change tracking
   - Log which attributes changed (audit trail)

3. Effective Date Management:
   - Current: Uses today's date
   - Production: Use actual change date from source (if available)
   - Handle backdating for late-arriving changes

4. Performance Optimization:
   - Partition dimension by is_current (separates active from historical)
   - Z-ORDER by member_id for fast lookups
   - Use broadcast join if current dimension fits in memory

5. Data Quality:
   - Validate no overlapping date ranges for same member
   - Check for orphaned records (multiple is_current = True)
   - Ensure surrogate keys are unique

6. Testing:
   - Unit test: New member creation
   - Unit test: Attribute change (plan_type change)
   - Unit test: No change (skip processing)
   - Integration test: Multiple changes over time

7. Monitoring:
   - Alert if change_rate exceeds threshold (data quality issue)
   - Track dimension size growth (storage planning)
   - Log processing time per batch

8. Backfill Strategy:
   - If reprocessing historical data, use actual change dates
   - May need to reconstruct history from source audit tables
   - Test backfill on copy of dimension first
"""