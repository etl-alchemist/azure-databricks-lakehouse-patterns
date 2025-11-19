"""
SCD Type 2 - Account Dimension (Finance)
=========================================
Track account attribute changes over time for financial analysis.

Use Case: Risk rating changes, pricing tier movements, relationship manager changes
Pattern: Close old record, insert new record with updated attributes

Author: Harsha Morram
Based on: LPL/Wells Fargo account/position tracking
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, lit, current_date, to_date, when, coalesce,
    max as spark_max, row_number, md5, concat_ws
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable
from datetime import datetime, timedelta

spark = SparkSession.builder.appName("SCD Type 2 - Account Dimension").getOrCreate()

# Configuration
SOURCE_PATH = "/mnt/silver/finance/account_master"
TARGET_DIM_PATH = "/mnt/gold/finance/dim_account"
HIGH_DATE = "9999-12-31"


def get_max_surrogate_key():
    """Get maximum surrogate key from dimension."""
    
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
        max_sk = dim_df.agg(spark_max("account_sk")).first()[0]
        return max_sk if max_sk else 0
    else:
        return 0


def load_source_data():
    """
    Load incoming account data from source system.
    Represents current state of accounts.
    """
    
    source_df = spark.read.format("delta").load(SOURCE_PATH)
    
    # Select attributes to track with SCD2
    tracked_attributes = [
        "account_number",        # Natural key
        "account_name",
        "account_type",          # Checking, Savings, Investment, Loan
        "account_status",        # Track: Active, Dormant, Closed changes
        "risk_rating",           # Track: AAA, AA, A, BBB, BB changes
        "pricing_tier",          # Track: Premium, Standard, Basic
        "relationship_manager",  # Track: RM changes
        "credit_limit",          # Track: Credit limit adjustments
        "interest_rate",         # Track: Rate changes
        "account_open_date",     # Static (Type 0 attribute)
        "annual_revenue"         # Track: For commercial accounts
    ]
    
    return source_df.select(*tracked_attributes)


def load_current_dimension():
    """Load current (active) dimension records."""
    
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
        return dim_df.filter(col("is_current") == True)
    else:
        return None


def identify_changes(source_df, current_dim_df):
    """
    Compare source to current dimension to detect changes.
    
    Change detection for financial attributes:
    - Risk rating change: Critical for regulatory reporting
    - Status change: Active ? Dormant ? Closed
    - Pricing tier: Impacts fee calculations
    - Credit limit: Impacts risk exposure
    """
    
    if current_dim_df is None:
        return source_df.withColumn("change_type", lit("NEW"))
    
    # Join on natural key
    comparison_df = source_df.alias("src").join(
        current_dim_df.alias("dim"),
        col("src.account_number") == col("dim.account_number"),
        "left"
    )
    
    # Hash of tracked attributes (excluding static attributes like account_open_date)
    comparison_df = comparison_df \
        .withColumn(
            "src_hash",
            md5(concat_ws("|",
                col("src.account_status"),
                col("src.risk_rating"),
                col("src.pricing_tier"),
                col("src.relationship_manager"),
                coalesce(col("src.credit_limit"), lit(0)),
                coalesce(col("src.interest_rate"), lit(0)),
                coalesce(col("src.annual_revenue"), lit(0))
            ))
        ) \
        .withColumn(
            "dim_hash",
            md5(concat_ws("|",
                coalesce(col("dim.account_status"), lit("")),
                coalesce(col("dim.risk_rating"), lit("")),
                coalesce(col("dim.pricing_tier"), lit("")),
                coalesce(col("dim.relationship_manager"), lit("")),
                coalesce(col("dim.credit_limit"), lit(0)),
                coalesce(col("dim.interest_rate"), lit(0)),
                coalesce(col("dim.annual_revenue"), lit(0))
            ))
        )
    
    # Classify changes
    classified_df = comparison_df \
        .withColumn(
            "change_type",
            when(col("dim.account_number").isNull(), "NEW")
            .when(col("src_hash") != col("dim_hash"), "CHANGED")
            .otherwise("NO_CHANGE")
        )
    
    result_df = classified_df.select(
        col("src.account_number"),
        col("src.account_name"),
        col("src.account_type"),
        col("src.account_status"),
        col("src.risk_rating"),
        col("src.pricing_tier"),
        col("src.relationship_manager"),
        col("src.credit_limit"),
        col("src.interest_rate"),
        col("src.account_open_date"),
        col("src.annual_revenue"),
        col("change_type"),
        col("dim.account_sk").alias("existing_account_sk")
    )
    
    return result_df


def process_new_accounts(new_accounts_df, start_surrogate_key):
    """Process new accounts."""
    
    print(f"Processing {new_accounts_df.count()} new accounts...")
    
    window_spec = Window.orderBy("account_number")
    
    new_records = new_accounts_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("account_sk", col("row_num") + start_surrogate_key) \
        .withColumn("effective_start_date", current_date()) \
        .withColumn("effective_end_date", to_date(lit(HIGH_DATE))) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_timestamp", current_date()) \
        .withColumn("updated_timestamp", current_date()) \
        .drop("row_num", "change_type", "existing_account_sk")
    
    return new_records


def process_changed_accounts(changed_accounts_df, start_surrogate_key):
    """Process changed accounts - close old, insert new."""
    
    print(f"Processing {changed_accounts_df.count()} changed accounts...")
    
    window_spec = Window.orderBy("account_number")
    
    new_versions = changed_accounts_df \
        .withColumn("row_num", row_number().over(window_spec)) \
        .withColumn("account_sk", col("row_num") + start_surrogate_key) \
        .withColumn("effective_start_date", current_date()) \
        .withColumn("effective_end_date", to_date(lit(HIGH_DATE))) \
        .withColumn("is_current", lit(True)) \
        .withColumn("created_timestamp", current_date()) \
        .withColumn("updated_timestamp", current_date()) \
        .drop("row_num", "change_type", "existing_account_sk")
    
    records_to_close = changed_accounts_df.select(
        "account_number",
        "existing_account_sk"
    ).withColumn("close_date", current_date())
    
    return new_versions, records_to_close


def apply_scd2_changes(new_records, records_to_close):
    """Apply SCD2 changes using Delta MERGE."""
    
    dim_table = DeltaTable.forPath(spark, TARGET_DIM_PATH)
    
    # Close old records
    if records_to_close and records_to_close.count() > 0:
        print("Closing old account records...")
        
        yesterday = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
        
        dim_table.alias("target").merge(
            records_to_close.alias("source"),
            "target.account_sk = source.existing_account_sk"
        ).whenMatchedUpdate(set={
            "effective_end_date": lit(yesterday),
            "is_current": lit(False),
            "updated_timestamp": current_date()
        }).execute()
    
    # Insert new records
    if new_records.count() > 0:
        print(f"Inserting {new_records.count()} new dimension records...")
        
        new_records.write \
            .format("delta") \
            .mode("append") \
            .save(TARGET_DIM_PATH)


def process_scd2_account_dimension():
    """Main SCD Type 2 orchestration for account dimension."""
    
    print("Starting SCD Type 2 account dimension processing...")
    
    source_df = load_source_data()
    print(f"Loaded {source_df.count()} accounts from source")
    
    current_dim_df = load_current_dimension()
    
    classified_df = identify_changes(source_df, current_dim_df)
    
    new_accounts = classified_df.filter(col("change_type") == "NEW")
    changed_accounts = classified_df.filter(col("change_type") == "CHANGED")
    no_change = classified_df.filter(col("change_type") == "NO_CHANGE")
    
    print(f"\nChange summary:")
    print(f"  - New accounts: {new_accounts.count()}")
    print(f"  - Changed accounts: {changed_accounts.count()}")
    print(f"  - No change: {no_change.count()}")
    
    current_max_sk = get_max_surrogate_key()
    
    new_records = None
    records_to_close = None
    
    if new_accounts.count() > 0:
        new_records = process_new_accounts(new_accounts, current_max_sk + 1)
        current_max_sk += new_accounts.count()
    
    if changed_accounts.count() > 0:
        new_versions, records_to_close = process_changed_accounts(
            changed_accounts, 
            current_max_sk + 1
        )
        
        if new_records:
            new_records = new_records.union(new_versions)
        else:
            new_records = new_versions
    
    if DeltaTable.isDeltaTable(spark, TARGET_DIM_PATH):
        apply_scd2_changes(new_records, records_to_close)
    else:
        print("Creating dimension table...")
        if new_records:
            new_records.write \
                .format("delta") \
                .mode("overwrite") \
                .partitionBy("is_current") \
                .save(TARGET_DIM_PATH)
    
    spark.sql(f"CREATE TABLE IF NOT EXISTS gold.dim_account USING DELTA LOCATION '{TARGET_DIM_PATH}'")
    
    print("\nSCD Type 2 processing completed")
    
    return {
        "new_accounts": new_accounts.count() if new_accounts else 0,
        "changed_accounts": changed_accounts.count() if changed_accounts else 0,
        "no_change": no_change.count() if no_change else 0
    }


def analyze_risk_rating_changes():
    """
    Business analysis: Track risk rating changes over time.
    Useful for credit risk monitoring and regulatory reporting.
    """
    
    dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
    
    # Show accounts with risk rating downgrades
    risk_changes = dim_df \
        .select(
            "account_number",
            "risk_rating",
            "effective_start_date",
            "is_current"
        ) \
        .orderBy("account_number", "effective_start_date")
    
    print("\nRisk rating history:")
    risk_changes.show(20, truncate=False)
    
    return risk_changes


def query_account_history(account_number):
    """View complete history for an account."""
    
    dim_df = spark.read.format("delta").load(TARGET_DIM_PATH)
    
    history = dim_df \
        .filter(col("account_number") == account_number) \
        .select(
            "account_sk",
            "account_number",
            "account_status",
            "risk_rating",
            "pricing_tier",
            "credit_limit",
            "effective_start_date",
            "effective_end_date",
            "is_current"
        ) \
        .orderBy("effective_start_date")
    
    print(f"\nHistory for account {account_number}:")
    history.show(truncate=False)
    
    return history


if __name__ == "__main__":
    """Execute SCD Type 2 account dimension processing."""
    
    try:
        stats = process_scd2_account_dimension()
        
        print(f"\n? SCD Type 2 processing complete:")
        print(f"  - New accounts added: {stats['new_accounts']}")
        print(f"  - Accounts updated: {stats['changed_accounts']}")
        print(f"  - Accounts unchanged: {stats['no_change']}")
        
        # Example queries
        # analyze_risk_rating_changes()
        # query_account_history("ACC001")
        
    except Exception as e:
        print(f"\n? SCD Type 2 processing failed: {str(e)}")
        raise
    
    finally:
        spark.stop()


"""
FINANCE-SPECIFIC CONSIDERATIONS:

1. Risk Rating Changes:
   - Critical for regulatory capital calculations (Basel III)
   - Track rating agency changes separately (Moody's, S&P, Fitch)
   - Alert on downgrades below investment grade (BBB-)

2. Credit Limit Tracking:
   - Impacts credit risk exposure calculations
   - Need audit trail for limit increase approvals
   - Track utilization ratio at each limit change

3. Pricing Tier Changes:
   - Affects fee calculations retroactively
   - May need to recalculate historical fees on tier change
   - Track reason for tier change (upgrade, downgrade, retention)

4. Account Status Lifecycle:
   - New ? Active ? Dormant (90 days no activity) ? Closed
   - Each transition has compliance implications
   - Track days in each status for reporting

5. Relationship Manager Changes:
   - Track for commission attribution
   - Impacts customer retention metrics
   - May trigger client notification requirements

6. Regulatory Reporting:
   - FFIEC Call Reports require point-in-time risk ratings
   - Stress testing needs historical credit limits
   - AML monitoring requires relationship manager history

7. Performance:
   - Partition by account_status for active vs. closed queries
   - Z-ORDER by account_number, risk_rating for common filters
   - Consider separate dimensions for high-churn attributes

8. Data Quality:
   - Validate risk ratings against approved list
   - Ensure credit_limit changes have approval workflow
   - Check for logical inconsistencies (closed account with active status)
"""