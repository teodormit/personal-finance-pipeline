-- =============================================================================
-- Personal Finance Warehouse - Postgres Init Blueprint
-- =============================================================================
-- Purpose: Public, sanitized reference of the database schema.
--          The real init_scripts/*.sql files are gitignored because they
--          contain owner-specific seed data and grants. This blueprint shows
--          the architecture and table shapes for portfolio readers and for
--          anyone forking the project.
--
-- Layout: staging  -> bronze -> silver -> gold + metadata sidecar
--
-- To run a real instance, copy this into your own numbered files under
-- init_scripts/ (Docker mounts it at /docker-entrypoint-initdb.d) and
-- replace :admin_role with your own Postgres role.
-- =============================================================================

\set admin_role pipeline_admin

-- -----------------------------------------------------------------------------
-- 1. Schemas
-- -----------------------------------------------------------------------------
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS metadata;

COMMENT ON SCHEMA staging  IS 'Transient landing zone; truncated each run';
COMMENT ON SCHEMA bronze   IS 'Immutable raw archive; append-only';
COMMENT ON SCHEMA silver   IS 'Cleaned, deduplicated, enriched analytics base';
COMMENT ON SCHEMA gold     IS 'Transaction-level intelligence (scores, labels)';
COMMENT ON SCHEMA metadata IS 'Pipeline run logs and data quality issues';

-- -----------------------------------------------------------------------------
-- 2. Staging - mirrors source export structure (all TEXT, parsed downstream)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS staging.raw_transactions (
    staging_id        SERIAL PRIMARY KEY,
    date              TEXT,
    note              TEXT,
    type              TEXT,
    payee             TEXT,
    amount            TEXT,
    labels            TEXT,
    account           TEXT,
    category          TEXT,
    currency          TEXT,
    payment           TEXT,
    source_file       VARCHAR(255),
    source_row_number INTEGER,
    loaded_at         TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id          UUID
);

-- -----------------------------------------------------------------------------
-- 3. Bronze - parsed, validated, never modified
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS bronze.transactions_raw (
    raw_id                BIGSERIAL PRIMARY KEY,
    transaction_date      DATE        NOT NULL,
    description           TEXT,
    transaction_type      VARCHAR(50) NOT NULL CHECK (transaction_type IN ('EXPENSE', 'INCOME')),
    payee                 VARCHAR(255),
    amount                NUMERIC(12, 2) NOT NULL,
    labels                VARCHAR(255),
    account_name          VARCHAR(100),
    subcategory           VARCHAR(100) NOT NULL,
    currency              VARCHAR(10)  NOT NULL,
    payment_method        VARCHAR(100),
    source_file           VARCHAR(255) NOT NULL,
    source_row_number     INTEGER,
    ingestion_timestamp   TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP,
    ingestion_batch_id    UUID         NOT NULL,
    has_quality_issues    BOOLEAN      DEFAULT FALSE,
    quality_issue_details JSONB
);

CREATE INDEX IF NOT EXISTS idx_bronze_date     ON bronze.transactions_raw(transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_bronze_batch_id ON bronze.transactions_raw(ingestion_batch_id);

-- -----------------------------------------------------------------------------
-- 4. Silver - analytics base, deduplicated by transaction_hash
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS silver.category_mapping (
    mapping_id     SERIAL PRIMARY KEY,
    subcategory    VARCHAR(100) NOT NULL UNIQUE,
    category       VARCHAR(100) NOT NULL,
    classification VARCHAR(20)  DEFAULT 'NEED' CHECK (classification IN ('NEED', 'WANT', 'MUST')),
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Sample seed rows (extend with the subcategories your source actually emits)
INSERT INTO silver.category_mapping (subcategory, category, classification) VALUES
    ('Groceries',     'Food & Drinks',        'NEED'),
    ('Bar, cafe',     'Food & Drinks',        'WANT'),
    ('Public transport', 'Transportation',    'NEED'),
    ('Rent',          'Housing',              'MUST'),
    ('Clothes & shoes', 'Shopping',           'WANT'),
    ('Wage, invoices', 'Income',              'NEED')
ON CONFLICT (subcategory) DO NOTHING;

CREATE TABLE IF NOT EXISTS silver.transactions (
    transaction_id       BIGSERIAL PRIMARY KEY,
    transaction_hash     VARCHAR(64) UNIQUE NOT NULL,            -- SHA-256(date|amount|subcategory|description)
    transaction_date     DATE        NOT NULL,
    transaction_type     VARCHAR(50) NOT NULL CHECK (transaction_type IN ('EXPENSE', 'INCOME')),

    -- Original currency
    amount               NUMERIC(12, 2) NOT NULL,
    amount_abs           NUMERIC(12, 2) NOT NULL,
    currency             VARCHAR(10)    NOT NULL,

    -- Multi-currency conversions
    amount_eur           NUMERIC(12, 2),
    amount_abs_eur       NUMERIC(12, 2),
    eur_conversion_rate  NUMERIC(10, 6) DEFAULT 1.0,
    amount_bgn           NUMERIC(12, 2),
    amount_abs_bgn       NUMERIC(12, 2),

    -- Source identifiers (NULL for CSV imports)
    source_record_id     VARCHAR(64),
    category_id          VARCHAR(64),

    description          TEXT,
    payee                VARCHAR(255),

    subcategory          VARCHAR(100) NOT NULL,
    category             VARCHAR(100),                            -- enriched via category_mapping
    classification       VARCHAR(20),

    account_name         VARCHAR(100),
    payment_method       VARCHAR(100),
    labels               VARCHAR(255),

    -- Derived calendar fields for analytics
    year                 INTEGER     NOT NULL,
    month                INTEGER     NOT NULL,
    quarter              INTEGER     NOT NULL,
    year_month           VARCHAR(7)  NOT NULL,
    day_of_week          INTEGER     NOT NULL,
    week_of_year         INTEGER     NOT NULL,
    is_weekend           BOOLEAN     NOT NULL,

    source_raw_id        BIGINT      REFERENCES bronze.transactions_raw(raw_id),
    created_at           TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    created_by           VARCHAR(100) DEFAULT 'etl_pipeline'
);

CREATE INDEX IF NOT EXISTS idx_silver_date ON silver.transactions(transaction_date DESC);
CREATE INDEX IF NOT EXISTS idx_silver_hash ON silver.transactions(transaction_hash);

-- -----------------------------------------------------------------------------
-- 5. Gold - transaction-level intelligence (scores upserted by transformers)
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS gold.transaction_notability (
    transaction_hash         VARCHAR(64) PRIMARY KEY,
    transaction_date         DATE NOT NULL,
    subcategory              VARCHAR(100),
    amount_abs_eur           NUMERIC(12, 2) NOT NULL,
    hist_window_days         INTEGER NOT NULL DEFAULT 365,
    hist_n_txns              INTEGER NOT NULL DEFAULT 0,
    hist_avg_amount_eur      NUMERIC(12, 2),
    hist_std_amount_eur      NUMERIC(12, 2),
    hist_max_amount_eur      NUMERIC(12, 2),
    amount_z_score           NUMERIC(12, 4),
    is_new_subcategory       BOOLEAN NOT NULL DEFAULT FALSE,
    is_new_subcategory_max   BOOLEAN NOT NULL DEFAULT FALSE,
    notability_score         NUMERIC(12, 4),                     -- max(z,0) + 4*new_subcategory + 2*new_record
    notability_label         VARCHAR(50),
    notability_reason        TEXT,
    computed_at              TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    extra_stats              JSONB DEFAULT '{}'::jsonb
);

CREATE TABLE IF NOT EXISTS gold.transaction_save_potential (
    transaction_hash        VARCHAR(64) PRIMARY KEY,
    transaction_date        DATE NOT NULL,
    subcategory             VARCHAR(100),
    classification          VARCHAR(10),
    amount_abs_eur          NUMERIC(12, 2) NOT NULL,
    avoidability            NUMERIC(4, 2) NOT NULL,              -- WANT=1.0, NEED=0.4, MUST=0.05
    month_txn_count         INTEGER NOT NULL,
    hist_avg_monthly_count  NUMERIC(8, 2),
    freq_ratio              NUMERIC(8, 2),
    freq_excess             NUMERIC(8, 2),
    amount_z_score          NUMERIC(12, 4),
    amt_excess              NUMERIC(12, 4),
    save_potential_score    NUMERIC(12, 4),                      -- 3*avoidability + 2*freq_excess + 1*amt_excess
    save_potential_label    VARCHAR(30),
    save_potential_reason   TEXT,
    computed_at             TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- -----------------------------------------------------------------------------
-- 6. Metadata - run logs and DQ tracking
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS metadata.pipeline_runs (
    run_id                   UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    run_timestamp            TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    source_file              VARCHAR(255),
    file_size_bytes          BIGINT,
    file_modified_date       TIMESTAMP,
    status                   VARCHAR(50) NOT NULL CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    rows_extracted           INTEGER,
    rows_staged              INTEGER,
    rows_loaded_bronze       INTEGER,
    rows_loaded_silver       INTEGER,
    rows_skipped_duplicates  INTEGER,
    rows_failed_validation   INTEGER,
    start_time               TIMESTAMP NOT NULL,
    end_time                 TIMESTAMP,
    duration_seconds         NUMERIC(10, 2),
    error_message            TEXT,
    stack_trace              TEXT
);

CREATE TABLE IF NOT EXISTS metadata.data_quality_issues (
    issue_id           SERIAL PRIMARY KEY,
    run_id             UUID REFERENCES metadata.pipeline_runs(run_id),
    issue_timestamp    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    severity           VARCHAR(20) NOT NULL CHECK (severity IN ('ERROR', 'WARNING', 'INFO')),
    issue_type         VARCHAR(100) NOT NULL,
    issue_description  TEXT NOT NULL,
    affected_row_data  JSONB,
    source_file        VARCHAR(255),
    source_row_number  INTEGER
);

-- -----------------------------------------------------------------------------
-- 7. Grants - replace :admin_role with your Postgres role before running
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
GRANT USAGE                           ON SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
