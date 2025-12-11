# Personal Finance Pipeline - Architecture & Design Document

**Version:** 1.0  
**Date:** December 2025  
**Author:** Data Engineering Team  
**Purpose:** Production-grade personal finance data pipeline with immutability, traceability, and data quality assurance

---

## Table of Contents
1. [System Overview](#system-overview)
2. [Data Architecture](#data-architecture)
3. [Data Modeling Strategy](#data-modeling-strategy)
4. [ETL Process Design](#etl-process-design)
5. [Data Quality Framework](#data-quality-framework)
6. [Immutability & Traceability](#immutability--traceability)
7. [Implementation Phases](#implementation-phases)

---

## 1. System Overview

### 1.1 Business Requirements

**Primary Goal:** Maintain an accurate, auditable, and up-to-date personal finance data warehouse for analytics and visualization.

**Key Requirements:**
- ✅ **Immutability**: Historical data never changes (append-only)
- ✅ **Traceability**: Full audit trail of all data changes
- ✅ **Data Quality**: Automated validation and cleansing
- ✅ **Idempotency**: Safe to re-run pipeline without duplicates
- ✅ **Incremental Loading**: Only process new/changed transactions
- ✅ **Point-in-Time Queries**: See data as it was at any historical date

### 1.2 Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        DATA SOURCES                              │
├─────────────────────────────────────────────────────────────────┤
│  • Wallet App Export (.xls/.xlsx)                               │
│  • Google Drive (manual storage)                                 │
│  • Historical Manual File (one-time migration)                   │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                    LANDING ZONE (MinIO)                          │
├─────────────────────────────────────────────────────────────────┤
│  • Raw files stored with timestamp                               │
│  • Bucket: finance-landing/                                      │
│  • Retention: 90 days                                            │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                  EXTRACTION LAYER (Python)                       │
├─────────────────────────────────────────────────────────────────┤
│  • Read Excel/CSV files                                          │
│  • Basic validation (file format, encoding)                      │
│  • Extract metadata (filename, timestamp, row count)             │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│               TRANSFORMATION LAYER (Python/Pandas)               │
├─────────────────────────────────────────────────────────────────┤
│  1. Data Quality Checks                                          │
│     • Date validation and parsing                                │
│     • Amount validation (numeric, reasonable range)              │
│     • Category standardization                                   │
│     • Required field presence                                    │
│                                                                  │
│  2. Data Enrichment                                              │
│     • Calculate derived fields (year, month, week)               │
│     • Generate transaction hash (for deduplication)              │
│     • Add processing metadata                                    │
│                                                                  │
│  3. Business Logic                                               │
│     • Filter out transfers (if specified)                        │
│     • Categorize income vs. expenses                             │
│     • Currency normalization                                     │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                   LOADING LAYER (PostgreSQL)                     │
├─────────────────────────────────────────────────────────────────┤
│  • Staging Tables (temporary, truncate-load)                     │
│  • Core Tables (immutable, append-only)                          │
│  • Audit Tables (full change history)                            │
│  • Aggregate Tables (pre-computed metrics)                       │
└────────────────────┬────────────────────────────────────────────┘
                     │
                     ▼
┌─────────────────────────────────────────────────────────────────┐
│                 CONSUMPTION LAYER (Tableau)                      │
├─────────────────────────────────────────────────────────────────┤
│  • Curated views optimized for Tableau                           │
│  • Monthly spending dashboards                                   │
│  • Category breakdowns                                           │
│  • Trend analysis                                                │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Data Architecture

### 2.1 Source Schema (Wallet App Export)

**File:** `Merged Interim Expenses - 2025 Aug.xlsx`

| Column Name | Data Type | Description | Nullable |
|-------------|-----------|-------------|----------|
| date | Date | Transaction date | No |
| note | String | Transaction description/memo | Yes |
| type | String | Transaction type (Expenses/Income) | No |
| payee | String | Merchant or recipient name | Yes |
| amount | Decimal | Transaction amount (negative=expense) | No |
| labels | String | User-defined tags/labels | Yes |
| account | String | Source account name | Yes |
| category | String | Expense/Income category | No |
| currency | String | Currency code (BGN, EUR, etc.) | No |
| payment | String | Payment method | Yes |

### 2.2 Target Schema (PostgreSQL)

We'll use a **medallion architecture** with staging → bronze → silver → gold layers.

---

## 3. Data Modeling Strategy

### 3.1 Design Principles

1. **Immutability**: All tables are append-only (no UPDATEs or DELETEs)
2. **Versioning**: Each transaction has a version history
3. **Auditability**: Every change is tracked with metadata
4. **Type 2 SCD**: Slowly Changing Dimensions for category/account changes
5. **Idempotency**: Pipeline can be re-run safely without duplicates

### 3.2 Database Schema Design

#### **Layer 1: Staging Tables** (Temporary)

**Purpose:** Temporary landing area for incoming data before validation.

```sql
-- Truncated and reloaded on each run
CREATE TABLE staging.raw_transactions (
    staging_id SERIAL PRIMARY KEY,
    -- Source data columns
    date DATE,
    note TEXT,
    type VARCHAR(50),
    payee VARCHAR(255),
    amount NUMERIC(12, 2),
    labels VARCHAR(255),
    account VARCHAR(100),
    category VARCHAR(100),
    currency VARCHAR(10),
    payment VARCHAR(100),
    
    -- Processing metadata
    source_file VARCHAR(255),
    source_row_number INTEGER,
    loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

#### **Layer 2: Bronze Tables** (Immutable Raw Data)

**Purpose:** Permanent storage of raw data exactly as received.

```sql
-- Append-only, never modified
CREATE TABLE bronze.transactions_raw (
    raw_id BIGSERIAL PRIMARY KEY,
    
    -- Source data (exactly as received)
    date DATE NOT NULL,
    note TEXT,
    type VARCHAR(50) NOT NULL,
    payee VARCHAR(255),
    amount NUMERIC(12, 2) NOT NULL,
    labels VARCHAR(255),
    account VARCHAR(100),
    category VARCHAR(100) NOT NULL,
    currency VARCHAR(10) NOT NULL,
    payment VARCHAR(100),
    
    -- Lineage metadata
    source_file VARCHAR(255) NOT NULL,
    source_row_number INTEGER,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingestion_batch_id UUID NOT NULL,
    
    -- Data quality flags
    has_quality_issues BOOLEAN DEFAULT FALSE,
    quality_issue_details JSONB
);

-- Indexes for performance
CREATE INDEX idx_bronze_date ON bronze.transactions_raw(date);
CREATE INDEX idx_bronze_ingestion_batch ON bronze.transactions_raw(ingestion_batch_id);
CREATE INDEX idx_bronze_source_file ON bronze.transactions_raw(source_file);
```

#### **Layer 3: Silver Tables** (Cleaned & Deduplicated)

**Purpose:** Cleaned, validated, and deduplicated transactions ready for analytics.

```sql
-- Main transaction table with business key
CREATE TABLE silver.transactions (
    transaction_id BIGSERIAL PRIMARY KEY,
    
    -- Business key for deduplication
    transaction_hash VARCHAR(64) UNIQUE NOT NULL,
    
    -- Transaction details
    transaction_date DATE NOT NULL,
    transaction_type VARCHAR(50) NOT NULL CHECK (transaction_type IN ('EXPENSE', 'INCOME')),
    amount NUMERIC(12, 2) NOT NULL,
    amount_abs NUMERIC(12, 2) NOT NULL, -- Absolute value for aggregations
    currency VARCHAR(10) NOT NULL,
    
    -- Descriptive fields
    description TEXT,
    payee VARCHAR(255),
    category VARCHAR(100) NOT NULL,
    subcategory VARCHAR(100),
    account_name VARCHAR(100),
    payment_method VARCHAR(100),
    labels VARCHAR(255),
    
    -- Derived fields for analytics
    year INTEGER NOT NULL,
    month INTEGER NOT NULL,
    quarter INTEGER NOT NULL,
    year_month VARCHAR(7) NOT NULL, -- 'YYYY-MM' for easy grouping
    day_of_week INTEGER NOT NULL,
    is_weekend BOOLEAN NOT NULL,
    
    -- Version control (Type 2 SCD)
    version INTEGER NOT NULL DEFAULT 1,
    is_current BOOLEAN NOT NULL DEFAULT TRUE,
    valid_from TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    valid_to TIMESTAMP DEFAULT '9999-12-31'::TIMESTAMP,
    
    -- Lineage
    source_raw_id BIGINT REFERENCES bronze.transactions_raw(raw_id),
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(100) DEFAULT 'etl_pipeline'
);

-- Indexes for query performance
CREATE INDEX idx_silver_date ON silver.transactions(transaction_date);
CREATE INDEX idx_silver_category ON silver.transactions(category);
CREATE INDEX idx_silver_year_month ON silver.transactions(year_month);
CREATE INDEX idx_silver_type ON silver.transactions(transaction_type);
CREATE INDEX idx_silver_is_current ON silver.transactions(is_current) WHERE is_current = TRUE;
CREATE INDEX idx_silver_valid_dates ON silver.transactions(valid_from, valid_to);
```

#### **Layer 4: Gold Tables** (Aggregated Analytics)

**Purpose:** Pre-computed aggregations for fast dashboard queries.

```sql
-- Monthly category summary
CREATE TABLE gold.monthly_category_summary (
    summary_id SERIAL PRIMARY KEY,
    year_month VARCHAR(7) NOT NULL,
    category VARCHAR(100) NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    
    transaction_count INTEGER NOT NULL,
    total_amount NUMERIC(12, 2) NOT NULL,
    avg_amount NUMERIC(12, 2) NOT NULL,
    min_amount NUMERIC(12, 2) NOT NULL,
    max_amount NUMERIC(12, 2) NOT NULL,
    
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (year_month, category, transaction_type)
);

-- Account balance history
CREATE TABLE gold.account_balance_history (
    balance_id SERIAL PRIMARY KEY,
    account_name VARCHAR(100) NOT NULL,
    balance_date DATE NOT NULL,
    
    opening_balance NUMERIC(12, 2) NOT NULL,
    total_income NUMERIC(12, 2) NOT NULL,
    total_expenses NUMERIC(12, 2) NOT NULL,
    closing_balance NUMERIC(12, 2) NOT NULL,
    
    calculated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (account_name, balance_date)
);
```

#### **Audit & Metadata Tables**

```sql
-- Pipeline execution log
CREATE TABLE metadata.pipeline_runs (
    run_id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    
    source_file VARCHAR(255),
    file_size_bytes BIGINT,
    file_modified_date TIMESTAMP,
    
    status VARCHAR(50) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    
    rows_extracted INTEGER,
    rows_staged INTEGER,
    rows_loaded_bronze INTEGER,
    rows_loaded_silver INTEGER,
    rows_skipped_duplicates INTEGER,
    rows_failed_validation INTEGER,
    
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds NUMERIC(10, 2),
    
    error_message TEXT,
    stack_trace TEXT
);

-- Data quality issues log
CREATE TABLE metadata.data_quality_issues (
    issue_id SERIAL PRIMARY KEY,
    run_id UUID REFERENCES metadata.pipeline_runs(run_id),
    
    issue_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('ERROR', 'WARNING', 'INFO')),
    
    issue_type VARCHAR(100) NOT NULL,
    issue_description TEXT NOT NULL,
    
    affected_row_data JSONB,
    source_file VARCHAR(255),
    source_row_number INTEGER
);

-- Transaction change audit
CREATE TABLE metadata.transaction_audit (
    audit_id BIGSERIAL PRIMARY KEY,
    transaction_id BIGINT NOT NULL,
    
    change_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    change_type VARCHAR(50) NOT NULL CHECK (change_type IN ('INSERT', 'UPDATE', 'DELETE', 'CORRECTION')),
    
    old_values JSONB,
    new_values JSONB,
    
    changed_by VARCHAR(100) NOT NULL,
    change_reason TEXT
);
```

---

## 4. ETL Process Design

### 4.1 Process Flow

```
1. EXTRACT
   ├── Download file from Google Drive (future)
   ├── Upload to MinIO landing bucket
   ├── Read Excel/CSV file
   ├── Basic format validation
   └── Generate processing batch ID

2. TRANSFORM
   ├── Load to staging.raw_transactions
   ├── Data Quality Validation
   │   ├── Date parsing and validation
   │   ├── Amount validation (not null, numeric, reasonable range)
   │   ├── Category validation (exists in master list)
   │   ├── Currency validation
   │   └── Required field checks
   ├── Data Cleansing
   │   ├── Trim whitespace
   │   ├── Standardize category names
   │   ├── Parse payment method
   │   └── Handle null values
   ├── Generate Business Key
   │   └── transaction_hash = SHA256(date || amount || category || description)
   └── Calculate Derived Fields
       ├── Extract year, month, quarter
       ├── Calculate absolute amount
       ├── Determine transaction type
       └── Calculate day of week

3. LOAD
   ├── Insert to bronze.transactions_raw (all records)
   ├── Check for duplicates using transaction_hash
   ├── Insert only new records to silver.transactions
   ├── Update gold tables (incremental aggregation)
   └── Archive source file to MinIO

4. VALIDATE
   ├── Reconciliation checks
   │   ├── Row count match (source vs. loaded)
   │   ├── Amount totals match
   │   └── Date range coverage
   └── Business rule validation
       ├── No future dates
       ├── No extreme amounts (> 1M or < -1M)
       └── Category distribution reasonable

5. FINALIZE
   ├── Update pipeline_runs table
   ├── Send notifications (if failures)
   └── Clean up staging tables
```

### 4.2 Deduplication Strategy

**Method:** Content-based hashing with business key

```python
def generate_transaction_hash(row):
    """
    Generate unique hash for transaction deduplication
    Uses: date + amount + category + first 50 chars of description
    """
    key_fields = [
        row['date'].strftime('%Y-%m-%d'),
        f"{row['amount']:.2f}",
        row['category'].strip().lower(),
        (row['note'] or '')[:50].strip().lower()
    ]
    
    key_string = '|'.join(key_fields)
    return hashlib.sha256(key_string.encode()).hexdigest()
```

**Deduplication Logic:**
1. Generate hash for incoming transaction
2. Check if hash exists in `silver.transactions`
3. If exists → Skip (log as duplicate)
4. If new → Insert to both bronze and silver
5. Track duplicate count in pipeline_runs

---

## 5. Data Quality Framework

### 5.1 Validation Rules

| Rule ID | Rule Name | Severity | Check |
|---------|-----------|----------|-------|
| DQ-001 | Date Not Null | ERROR | transaction_date IS NOT NULL |
| DQ-002 | Date Format Valid | ERROR | Date parseable and < today |
| DQ-003 | Date Not Future | WARNING | transaction_date <= CURRENT_DATE |
| DQ-004 | Amount Not Null | ERROR | amount IS NOT NULL |
| DQ-005 | Amount Numeric | ERROR | amount is valid NUMERIC |
| DQ-006 | Amount Reasonable | WARNING | amount BETWEEN -1000000 AND 1000000 |
| DQ-007 | Category Not Null | ERROR | category IS NOT NULL |
| DQ-008 | Category Valid | WARNING | category IN (master_category_list) |
| DQ-009 | Currency Valid | WARNING | currency IN ('BGN', 'EUR', 'USD', ...) |
| DQ-010 | Type Valid | ERROR | type IN ('Expenses', 'Income') |

### 5.2 Handling Quality Issues

**ERROR Severity:**
- Record is rejected
- Logged to `metadata.data_quality_issues`
- Flagged in bronze table with `has_quality_issues = TRUE`
- Not promoted to silver table

**WARNING Severity:**
- Record is loaded with flag
- Logged for review
- Can be corrected manually later

---

## 6. Immutability & Traceability

### 6.1 Immutability Principles

1. **Bronze Layer**: Never modify or delete raw data
2. **Silver Layer**: Use Type 2 SCD for corrections
   - Old version: `is_current = FALSE`, set `valid_to`
   - New version: Insert with `version += 1`, `is_current = TRUE`
3. **Gold Layer**: Recalculated, not updated
4. **Audit Trail**: Every change logged to `metadata.transaction_audit`

### 6.2 Correction Workflow

If a transaction needs correction (e.g., wrong category):

```sql
-- 1. Mark old version as not current
UPDATE silver.transactions
SET is_current = FALSE,
    valid_to = CURRENT_TIMESTAMP
WHERE transaction_id = <id>;

-- 2. Insert corrected version
INSERT INTO silver.transactions (...)
VALUES (..., version = old_version + 1, is_current = TRUE);

-- 3. Log the change
INSERT INTO metadata.transaction_audit (...)
VALUES (..., change_type = 'CORRECTION', ...);
```

### 6.3 Point-in-Time Queries

Query data as it existed at any historical date:

```sql
SELECT *
FROM silver.transactions
WHERE valid_from <= '2024-06-30'
  AND valid_to > '2024-06-30'
  AND is_current = FALSE
UNION ALL
SELECT *
FROM silver.transactions
WHERE is_current = TRUE;
```

---

## 7. Implementation Phases

### Phase 1: Initial Setup (Week 1)
- ✅ Create database schemas (staging, bronze, silver, gold, metadata)
- ✅ Create all tables with indexes
- ✅ Set up Python environment with required libraries
- ✅ Configure MinIO buckets

### Phase 2: Initial Data Load (Week 1)
- ✅ Load historical file (Merged Interim Expenses)
- ✅ Validate data quality
- ✅ Populate bronze and silver tables
- ✅ Generate initial gold aggregations
- ✅ Verify in Tableau

### Phase 3: Incremental Pipeline (Week 2)
- ✅ Build Python ETL scripts
- ✅ Implement deduplication logic
- ✅ Add data quality checks
- ✅ Create manual trigger script
- ✅ Test with sample incremental file

### Phase 4: Monitoring & Optimization (Week 3)
- ✅ Add comprehensive logging
- ✅ Create reconciliation reports
- ✅ Optimize queries and indexes
- ✅ Document operational procedures

### Phase 5: Future Enhancements (Later)
- 🔲 Google Drive API integration
- 🔲 Scheduled execution with Prefect
- 🔲 Email notifications
- 🔲 Web UI for data quality monitoring

---

## 8. Key Decisions & Rationale

| Decision | Rationale |
|----------|-----------|
| **Append-only architecture** | Financial data requires full audit trail |
| **Type 2 SCD for corrections** | Maintain history while allowing fixes |
| **Content-based deduplication** | More reliable than timestamp-based |
| **Medallion architecture** | Industry standard for data lakehouses |
| **PostgreSQL over NoSQL** | ACID compliance critical for financial data |
| **Pre-computed gold tables** | Tableau performance optimization |
| **Manual trigger (Phase 1)** | Simplicity and control during setup |

---

## 9. Success Metrics

**Data Quality:**
- 100% of transactions have valid dates and amounts
- < 1% data quality warnings
- Zero data loss from source to destination

**Performance:**
- Initial load: < 5 minutes for 5,000 transactions
- Incremental load: < 30 seconds for 100 new transactions
- Tableau dashboard refresh: < 10 seconds

**Reliability:**
- Pipeline success rate: > 99%
- Zero duplicate transactions in silver layer
- Full audit trail for all changes

---

**Document Version:** 1.0  
**Last Updated:** December 2024  
**Next Review:** After Phase 2 completion
