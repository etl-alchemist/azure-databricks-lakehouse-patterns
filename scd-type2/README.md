# SCD Type 2 - Slowly Changing Dimensions

**Track complete history of dimension changes with effective dating.**

## Overview

Slowly Changing Dimensions (SCD) is a data warehousing technique for handling changes to dimension attributes over time. SCD Type 2 maintains a full audit trail by creating new records for each change, preserving historical states.

## Why SCD Type 2?

### Business Need
- **Healthcare:** Track member plan changes, address updates, coverage gaps for HEDIS measures
- **Finance:** Track account risk ratings, pricing tiers, covenant compliance over time
- **Retail:** Track customer segment changes, loyalty tier movements
- **Any domain requiring:** "What was true at this point in time?"

### Example: Member Dimension

**Without SCD Type 2 (Type 1 - overwrite):**
```
| member_id | name        | plan_type | zip_code |
|-----------|-------------|-----------|----------|
| M001      | John Smith  | Gold      | 28104    |
```

Member moves and changes plan ? record gets updated:
```
| member_id | name        | plan_type | zip_code |
|-----------|-------------|-----------|----------|
| M001      | John Smith  | Silver    | 28105    |  ? Lost history!
```

**Problem:** Can't answer "What plan was this member on in January?" Historical analysis impossible.

---

**With SCD Type 2:**
```
| member_sk | member_id | name        | plan_type | zip_code | effective_start | effective_end   | is_current |
|-----------|-----------|-------------|-----------|----------|-----------------|-----------------|------------|
| 1         | M001      | John Smith  | Gold      | 28104    | 2023-01-01      | 2024-06-30      | False      |
| 2         | M001      | John Smith  | Silver    | 28105    | 2024-07-01      | 9999-12-31      | True       |
```

**Now you can answer:**
- "What plan was M001 on in March 2024?" ? Gold
- "When did M001 move to zip 28105?" ? July 1, 2024
- "Show all members who were on Gold plans in Q1 2024" ? Includes M001

---

## SCD Types Comparison

| Type | Description | Use Case | History Preserved? |
|------|-------------|----------|-------------------|
| **Type 0** | Never changes | Fixed attributes (SSN, DOB) | N/A |
| **Type 1** | Overwrite | Current state only, no history needed | ? No |
| **Type 2** | Add new row | Full history, point-in-time queries | ? Yes |
| **Type 3** | Add new column | Track one previous value only | ?? Limited |
| **Type 4** | Separate history table | Complex scenarios | ? Yes |
| **Type 6** | Hybrid (1+2+3) | Combined approach | ? Yes |

**Type 2 is the most common for production analytics.**

---

## Key Concepts

### Surrogate Key
A unique identifier for each dimension record (not the business key).

```python
member_sk = 1, 2, 3, ...  # Unique per row
member_id = "M001"        # Business key (can repeat across rows)
```

**Why?** Allows same member to have multiple historical records.

### Effective Dates
Track when each record was valid.

- **effective_start_date:** When this version became true
- **effective_end_date:** When this version stopped being true
  - Current record: `9999-12-31` or `NULL`
  - Historical record: Actual end date

### Is Current Flag
Boolean to quickly identify active records.

```sql
WHERE is_current = True  -- Fast filter for current state
```

**Alternative:** Some implementations use `effective_end_date IS NULL` instead.

### Natural Key (Business Key)
The real-world identifier (member_id, account_number, product_code).

Multiple rows share the same natural key but have different surrogate keys.

---

## Implementation Pattern

### New Record (Insert)
Member M002 joins with Gold plan:

```sql
INSERT INTO dim_member VALUES (
  3,              -- member_sk (new surrogate key)
  'M002',         -- member_id (natural key)
  'Jane Doe',
  'Gold',
  '28203',
  '2024-11-01',   -- effective_start_date
  '9999-12-31',   -- effective_end_date (current)
  True            -- is_current
)
```

---

### Update Record (Close Old, Insert New)
Member M001 changes from Gold to Silver on July 1:

**Step 1: Close the old record**
```sql
UPDATE dim_member
SET 
  effective_end_date = '2024-06-30',  -- Day before change
  is_current = False
WHERE 
  member_id = 'M001' 
  AND is_current = True
```

**Step 2: Insert new record**
```sql
INSERT INTO dim_member VALUES (
  4,              -- member_sk (new surrogate key)
  'M001',         -- member_id (same natural key)
  'John Smith',
  'Silver',       -- Changed attribute
  '28104',
  '2024-07-01',   -- effective_start_date (change date)
  '9999-12-31',   -- effective_end_date (now current)
  True            -- is_current
)
```

**Result:** Two records for M001, history preserved.

---

### No Change (Do Nothing)
If M001's attributes haven't changed, no new record is created.

**Check before insert:**
```python
# Compare incoming data with current record
if incoming_plan != current_plan or incoming_zip != current_zip:
    # Create new SCD2 record
else:
    # No change, skip
```

---

## Point-in-Time Queries

**Question:** "What plan was M001 on March 15, 2024?"

```sql
SELECT plan_type
FROM dim_member
WHERE 
  member_id = 'M001'
  AND '2024-03-15' BETWEEN effective_start_date AND effective_end_date
```

**Answer:** Gold

---

**Question:** "Show all members on Gold plans as of Q1 2024"

```sql
SELECT DISTINCT member_id
FROM dim_member
WHERE 
  plan_type = 'Gold'
  AND '2024-03-31' BETWEEN effective_start_date AND effective_end_date
```

---

## Common Patterns

### Pattern 1: Current State Only
```sql
SELECT * FROM dim_member WHERE is_current = True
```

Fast, no date filtering needed.

---

### Pattern 2: Historical Snapshot
```sql
SELECT * 
FROM dim_member
WHERE '2024-01-01' BETWEEN effective_start_date AND effective_end_date
```

"What did the dimension look like on Jan 1?"

---

### Pattern 3: Attribute Change History
```sql
SELECT 
  member_id,
  plan_type,
  effective_start_date,
  effective_end_date
FROM dim_member
WHERE member_id = 'M001'
ORDER BY effective_start_date
```

Shows full history of plan changes for one member.

---

### Pattern 4: Join Facts to Historical Dimensions
```sql
SELECT 
  f.claim_date,
  f.billed_amount,
  d.plan_type
FROM fact_claims f
JOIN dim_member d 
  ON f.member_sk = d.member_sk  -- Use surrogate key!
WHERE f.claim_date BETWEEN d.effective_start_date AND d.effective_end_date
```

**Critical:** Join on surrogate key AND validate effective dates.

---

## Attributes to Track with SCD2

### Healthcare (Member Dimension)
- ? Plan type (Gold, Silver, Bronze)
- ? Address/zip code (for network assignment)
- ? PCP assignment
- ? Risk score
- ? Name (rarely changes, not analytically important)
- ? DOB (never changes - Type 0)

### Finance (Account Dimension)
- ? Risk rating (AAA, BB, etc.)
- ? Account status (Active, Dormant, Closed)
- ? Pricing tier
- ? Relationship manager
- ? Account number (never changes - Type 0)
- ? Open date (never changes - Type 0)

**Rule of thumb:** Track attributes that change AND impact analytics.

---

## Best Practices

### 1. Choose Effective Date Granularity
- **Day-level:** Most common (YYYY-MM-DD)
- **Timestamp:** If intraday changes matter (trading systems)
- **Month-level:** For slower-changing attributes (rare)

**Don't over-engineer:** Day-level is usually sufficient.

---

### 2. Use Consistent End Date Convention
**Option A: High date (9999-12-31)**
```sql
WHERE effective_end_date = '9999-12-31'  -- Current records
```

**Option B: NULL**
```sql
WHERE effective_end_date IS NULL  -- Current records
```

Pick one and stick with it. Most teams use high date because:
- Works with BETWEEN queries
- No NULL handling logic
- Clear in reports

---

### 3. Surrogate Key Strategy
**Auto-increment:**
```python
member_sk = existing_max_sk + 1
```

**Hash-based (more complex but deterministic):**
```python
member_sk = hash(member_id + effective_start_date)
```

**Monotonically increasing (Spark):**
```python
from pyspark.sql.functions import monotonically_increasing_id
df.withColumn("member_sk", monotonically_increasing_id())
```

---

### 4. Handle Late-Arriving Changes
**Problem:** Change happened July 1, but you learn about it July 15.

**Solution:**
```python
# When closing old record, use the ACTUAL change date, not discovery date
effective_end_date = '2024-06-30'  # Day before change
effective_start_date = '2024-07-01'  # Actual change date

# NOT:
effective_end_date = '2024-07-14'  # Discovery date - WRONG!
```

Backdate to preserve accurate history.

---

### 5. Fact Table Design
**Facts should reference surrogate key, not natural key:**

```python
# Good
fact_claims.member_sk ? dim_member.member_sk

# Bad
fact_claims.member_id ? dim_member.member_id  # Which version?
```

**At load time:**
```python
# Join fact to dimension using natural key + effective dates
fact.claim_date BETWEEN dim.effective_start_date AND dim.effective_end_date
```

This assigns the correct `member_sk` based on when the claim occurred.

---

### 6. Optimize for Query Performance
```sql
-- Partition by is_current for fast current-state queries
CREATE TABLE dim_member
PARTITIONED BY (is_current)

-- Z-ORDER on natural key for historical lookups
OPTIMIZE dim_member ZORDER BY (member_id)
```

---

## Common Pitfalls

### ? Pitfall 1: Overlapping Date Ranges
**Wrong:**
```
| member_sk | member_id | plan  | start      | end        |
|-----------|-----------|-------|------------|------------|
| 1         | M001      | Gold  | 2024-01-01 | 2024-07-01 |
| 2         | M001      | Silver| 2024-07-01 | 9999-12-31 |
```

**Problem:** July 1 appears in BOTH records (BETWEEN is inclusive).

**Correct:**
```
| member_sk | member_id | plan  | start      | end        |
|-----------|-----------|-------|------------|------------|
| 1         | M001      | Gold  | 2024-01-01 | 2024-06-30 |  ? End day BEFORE change
| 2         | M001      | Silver| 2024-07-01 | 9999-12-31 |
```

---

### ? Pitfall 2: Forgetting to Close Old Record
**Wrong:**
```python
# Just insert new record, forget to update old one
insert_new_record()
```

**Result:** Two `is_current = True` records for same member!

**Correct:**
```python
# Always: close old, then insert new
close_current_record()
insert_new_record()
```

---

### ? Pitfall 3: Creating Records for Non-Changes
**Wrong:**
```python
# Daily batch checks member data, sees no change
# But still creates new SCD2 record with same attributes
```

**Result:** Massive table bloat, 365+ records per member per year.

**Correct:**
```python
if current_attributes != incoming_attributes:
    create_scd2_record()
else:
    skip  # No change, no new record
```

---

### ? Pitfall 4: Using Natural Key in Fact Foreign Keys
**Wrong:**
```sql
-- Fact table
claim_id, member_id, claim_date, amount

-- Query (BROKEN for SCD2)
SELECT * 
FROM fact_claims f
JOIN dim_member d ON f.member_id = d.member_id
WHERE d.is_current = True
```

**Problem:** Shows current plan, not plan at claim time!

**Correct:**
```sql
-- Fact table (use surrogate key)
claim_id, member_sk, claim_date, amount

-- Query
SELECT * 
FROM fact_claims f
JOIN dim_member d ON f.member_sk = d.member_sk
```

Fact already has correct historical surrogate key embedded.

---

## Testing SCD Type 2

### Test Case 1: New Member
```python
def test_new_member():
    # Given: No existing record for M003
    # When: Insert M003 with plan Gold
    # Then: 
    #   - One record created
    #   - is_current = True
    #   - effective_end_date = 9999-12-31
```

### Test Case 2: Attribute Change
```python
def test_member_plan_change():
    # Given: M001 on Gold plan
    # When: Update to Silver effective 2024-07-01
    # Then:
    #   - Old record: is_current = False, end_date = 2024-06-30
    #   - New record: is_current = True, start_date = 2024-07-01
    #   - Two total records for M001
```

### Test Case 3: No Change
```python
def test_no_change():
    # Given: M001 on Gold plan
    # When: Process same attributes again
    # Then:
    #   - No new record created
    #   - Still one current record
```

### Test Case 4: Multiple Changes
```python
def test_multiple_changes():
    # Given: M001 history: Gold ? Silver ? Bronze
    # When: Query for plan on specific dates
    # Then:
    #   - 2024-03-15: Gold
    #   - 2024-08-15: Silver
    #   - 2024-11-15: Bronze
```

---

## Files in This Pattern

1. **`README.md`** - This file
2. **`scd2_member_dimension.py`** - Healthcare member tracking (plan changes, address, PCP)
3. **`scd2_account_dimension.py`** - Finance account tracking (risk rating, status, pricing tier)

---

## Performance Considerations

### For 10M+ dimension records:
- **Partition by is_current** - Separates active from historical
- **Z-ORDER by natural key** - Fast lookups by member_id, account_id
- **Cluster by effective dates** - If frequent date-range queries
- **Incremental MERGE** - Don't full-scan dimension daily

### Query optimization:
```sql
-- Fast: Partition pruning
SELECT * FROM dim_member WHERE is_current = True

-- Slower: Full scan with date filter
SELECT * FROM dim_member 
WHERE '2024-11-17' BETWEEN effective_start_date AND effective_end_date
```

### Storage costs:
- SCD2 tables grow larger than Type 1 (historical records accumulate)
- Typical: 3-5x size vs. Type 1
- Implement retention policy: Archive records > 7 years old

---

## Integration with Power BI

**Scenario:** Show current member plan type

```DAX
Current Members = 
CALCULATE(
    COUNTROWS(DimMember),
    DimMember[is_current] = TRUE
)
```

**Scenario:** Show plan distribution as of specific date

```DAX
Historical Plan Mix = 
CALCULATE(
    COUNTROWS(DimMember),
    DimMember[effective_start_date] <= SelectedDate,
    DimMember[effective_end_date] >= SelectedDate
)
```

**Tip:** Create "Current" and "Historical" views for Power BI:
- `dim_member_current`: WHERE is_current = True (fast)
- `dim_member_historical`: Full table (for point-in-time analysis)

---

## Next Steps

After mastering SCD Type 2:
1. **Data Quality** - Add validation for date overlaps, orphaned records (see `/data-quality/`)
2. **Optimization** - Tune SCD2 MERGE operations for scale (see `/optimization/`)
3. **Advanced:** SCD Type 6 (hybrid) for complex scenarios

---

## References

- [Kimball SCD Techniques](http://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/)
- [Delta Lake MERGE Operations](https://docs.delta.io/latest/delta-update.html#upsert-into-a-table-using-merge)
- [Databricks SCD Type 2 Example](https://docs.databricks.com/delta/merge-into-scd-type-2.html)