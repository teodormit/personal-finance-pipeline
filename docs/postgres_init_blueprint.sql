-- =============================================================================
-- Personal Finance Warehouse - Complete Postgres Schema
-- =============================================================================
-- This single file reconstructs the entire warehouse from scratch: all five
-- schemas (staging -> bronze -> silver -> gold, plus the metadata sidecar),
-- every table and index, and the silver audit trigger. Running it against an
-- empty database yields the exact architecture the pipeline expects.
--
--   Layout:  staging -> bronze -> silver -> gold   (+ metadata sidecar)
--
-- Run it once against a fresh, empty database:
--
--   createdb finance_warehouse
--   psql -d finance_warehouse -f docs/postgres_init_blueprint.sql
--
-- Notes:
--   * This warehouse is modelled against the BudgetBakers (Wallet) data shape:
--     the staging columns mirror the Wallet CSV/XLSX export, and the
--     category_mapping taxonomy below is Wallet's full category/subcategory set.
--   * The owning role defaults to `pipeline_admin` (see the \set just below).
--     Either create that role first, or edit the \set line to your own role.
--   * The category_mapping seed below is the complete classification taxonomy
--     the live warehouse uses - WANT/NEED/MUST per subcategory, NULL for income
--     rows (income carries no avoidability classification).
--   * Owner-specific seed data lives in private scripts/sql/ files that are not
--     part of the public repo; this blueprint is the canonical, sanitized
--     schema of record and is kept in sync with the live warehouse.
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
    ingestion_batch_id    UUID         NOT NULL
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
    -- NULL is allowed: income subcategories carry no NEED/WANT/MUST classification.
    classification VARCHAR(20)  DEFAULT 'NEED' CHECK (classification IS NULL OR classification IN ('NEED', 'WANT', 'MUST')),
    created_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP,
    updated_at     TIMESTAMP    DEFAULT CURRENT_TIMESTAMP
);

-- Full classification taxonomy, grouped by BudgetBakers (Wallet) category.
-- Income subcategories carry NULL classification (no avoidability meaning).
INSERT INTO silver.category_mapping (subcategory, category, classification) VALUES
    -- Communication, PC
    ('Internet',                    'Communication, PC',    'NEED'),
    ('Phone, cell phone',           'Communication, PC',    'NEED'),
    ('Postal services',             'Communication, PC',    'NEED'),
    ('Software, apps, games',       'Communication, PC',    'WANT'),
    -- Financial expenses
    ('Advisory',                    'Financial expenses',   'MUST'),
    ('Charges, Fees',               'Financial expenses',   'MUST'),
    ('Child Support',               'Financial expenses',   'MUST'),
    ('Fines',                       'Financial expenses',   'MUST'),
    ('Insurances',                  'Financial expenses',   'MUST'),
    ('Loan, interests',             'Financial expenses',   'MUST'),
    ('Taxes',                       'Financial expenses',   'MUST'),
    -- Food & Drinks
    ('Food & Drinks',               'Food & Drinks',        'NEED'),
    ('Groceries',                   'Food & Drinks',        'NEED'),
    ('Bar, cafe',                   'Food & Drinks',        'WANT'),
    -- Housing
    ('Energy, utilities',           'Housing',              'MUST'),
    ('Maintenance, repairs',        'Housing',              'MUST'),
    ('Mortgage',                    'Housing',              'MUST'),
    ('Property insurance',          'Housing',              'MUST'),
    ('Rent',                        'Housing',              'MUST'),
    ('Services',                    'Housing',              'MUST'),
    -- Income (classification NULL by design)
    ('Aliments',                    'Income',               NULL),
    ('Bonus, extra income',         'Income',               NULL),
    ('Checks, coupons',             'Income',               NULL),
    ('Dues & grants',               'Income',               NULL),
    ('Gifts',                       'Income',               NULL),
    ('Interests, dividends',        'Income',               NULL),
    ('Investment income',           'Income',               NULL),
    ('Lending, renting',            'Income',               NULL),
    ('Other income',                'Income',               NULL),
    ('Refunds',                     'Income',               NULL),
    ('Refunds (tax, purchase)',     'Income',               NULL),
    ('Rental income',               'Income',               NULL),
    ('Sale',                        'Income',               NULL),
    ('Wage, invoices',              'Income',               NULL),
    -- Investments
    ('Collections',                 'Investments',          'WANT'),
    ('Cryptocurrency',              'Investments',          'WANT'),
    ('Financial investments',       'Investments',          'WANT'),
    ('Investments',                 'Investments',          'WANT'),
    ('Realty',                      'Investments',          'WANT'),
    ('Savings',                     'Investments',          'WANT'),
    ('Stocks, bonds',               'Investments',          'WANT'),
    ('Vehicles, chattels',          'Investments',          'WANT'),
    -- Life & Entertainment
    ('Active sport, fitness',       'Life & Entertainment', 'WANT'),
    ('Alcohol, tobacco',            'Life & Entertainment', 'WANT'),
    ('Books, audio, subscriptions', 'Life & Entertainment', 'WANT'),
    ('Charity, gifts',              'Life & Entertainment', 'WANT'),
    ('Culture, sport events',       'Life & Entertainment', 'WANT'),
    ('Education, development',       'Life & Entertainment', 'WANT'),
    ('Health care, doctor',         'Life & Entertainment', 'WANT'),
    ('Hobbies',                     'Life & Entertainment', 'WANT'),
    ('Holiday, trips, hotels',      'Life & Entertainment', 'WANT'),
    ('Life events',                 'Life & Entertainment', 'WANT'),
    ('Lottery, gambling',           'Life & Entertainment', 'WANT'),
    ('TV, Streaming',               'Life & Entertainment', 'WANT'),
    ('Wellness, beauty',            'Life & Entertainment', 'WANT'),
    -- Other
    ('Other',                       'Other',                'WANT'),
    ('Uncategorized',               'Other',                'WANT'),
    ('Unknown Expense',             'Other',                'WANT'),
    ('UNKNOWNN_CATEGORY',           'Other',                'WANT'),
    -- Shopping
    ('Clothes & shoes',             'Shopping',             'WANT'),
    ('Drug-store, chemist',         'Shopping',             'WANT'),
    ('Electronics, accessories',    'Shopping',             'WANT'),
    ('Free time',                   'Shopping',             'WANT'),
    ('Gifts, joy',                  'Shopping',             'WANT'),
    ('Health and beauty',           'Shopping',             'WANT'),
    ('Home, garden',                'Shopping',             'WANT'),
    ('Jewels, accessories',         'Shopping',             'WANT'),
    ('Kids',                        'Shopping',             'WANT'),
    ('Pets, animals',               'Shopping',             'WANT'),
    ('Stationary, tools',           'Shopping',             'WANT'),
    -- Transfers
    ('Transfer',                    'Transfers',            'WANT'),
    ('Transfer, withdraw',          'Transfers',            'WANT'),
    -- Transportation
    ('Business trips',              'Transportation',       'NEED'),
    ('Long distance',               'Transportation',       'NEED'),
    ('Public transport',            'Transportation',       'NEED'),
    ('Taxi',                        'Transportation',       'NEED'),
    -- Vehicle
    ('Fuel',                        'Vehicle',              'NEED'),
    ('Leasing',                     'Vehicle',              'NEED'),
    ('Parking',                     'Vehicle',              'NEED'),
    ('Rentals',                     'Vehicle',              'NEED'),
    ('Vehicle insurance',           'Vehicle',              'NEED'),
    ('Vehicle maintenance',         'Vehicle',              'NEED')
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

    -- Income semantics: NULL = expense row; REAL = genuine income stream
    -- (salary, dividends, child support); REFUND = expense offset logged as
    -- income in the source (reimbursements, purchase refunds).
    income_type          VARCHAR(10)  DEFAULT NULL CHECK (income_type IS NULL OR income_type IN ('REAL', 'REFUND')),

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
-- 6. Metadata - pipeline run logs
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

-- -----------------------------------------------------------------------------
-- 7. Audit log - immutable change log for silver.transactions
-- -----------------------------------------------------------------------------
-- Records every out-of-band UPDATE/DELETE of a settled silver row (including
-- manual SQL corrections). INSERTs are not audited - bronze.transactions_raw
-- plus created_at/created_by already lineage ingestion. Pipeline writes are
-- excluded via the `audit.suppress` session flag (set by the loaders). SCD
-- Type 2 was deliberately rejected in favour of this log.
CREATE TABLE IF NOT EXISTS metadata.transaction_audit (
    audit_id          BIGSERIAL PRIMARY KEY,
    transaction_id    BIGINT      NOT NULL,                       -- silver id at change time; not stable across a full rebuild
    transaction_hash  VARCHAR(64) NOT NULL,                       -- stable business key; follow a row across its lifetime
    change_timestamp  TIMESTAMP   NOT NULL DEFAULT CURRENT_TIMESTAMP,
    change_type       VARCHAR(10) NOT NULL CHECK (change_type IN ('UPDATE', 'DELETE')),
    changed_fields    TEXT[],                                     -- columns whose value changed (UPDATE only)
    old_values        JSONB,                                      -- UPDATE: changed cols before; DELETE: the whole removed row
    new_values        JSONB,                                      -- UPDATE: changed cols after; DELETE: NULL
    changed_by        VARCHAR(100) NOT NULL,                      -- audit.actor session setting, else the DB role
    change_reason     TEXT                                        -- optional audit.reason session setting
);

CREATE INDEX IF NOT EXISTS idx_transaction_audit_hash ON metadata.transaction_audit(transaction_hash);
CREATE INDEX IF NOT EXISTS idx_transaction_audit_time ON metadata.transaction_audit(change_timestamp DESC);

-- Trigger function: diffs OLD vs NEW, logs only the columns that actually
-- changed, and skips pipeline ingestion when audit.suppress = 'on'.
CREATE OR REPLACE FUNCTION metadata.fn_audit_transaction_change()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_actor    TEXT := coalesce(current_setting('audit.actor', true), current_user);
    v_reason   TEXT := current_setting('audit.reason', true);
    v_old      JSONB;
    v_new      JSONB;
    v_changed  TEXT[] := ARRAY[]::TEXT[];
    v_old_diff JSONB := '{}'::JSONB;
    v_new_diff JSONB := '{}'::JSONB;
    k          TEXT;
BEGIN
    -- Pipeline ingestion is not an auditable correction - skip it.
    IF current_setting('audit.suppress', true) = 'on' THEN
        RETURN NULL;
    END IF;

    IF TG_OP = 'DELETE' THEN
        INSERT INTO metadata.transaction_audit
            (transaction_id, transaction_hash, change_type,
             changed_fields, old_values, new_values, changed_by, change_reason)
        VALUES
            (OLD.transaction_id, OLD.transaction_hash, 'DELETE',
             NULL, to_jsonb(OLD), NULL, v_actor, v_reason);
        RETURN NULL;
    END IF;

    -- TG_OP = 'UPDATE': diff old vs new, log only the columns that changed.
    v_old := to_jsonb(OLD);
    v_new := to_jsonb(NEW);

    FOR k IN SELECT jsonb_object_keys(v_new) LOOP
        IF (v_old -> k) IS DISTINCT FROM (v_new -> k) THEN
            v_changed  := array_append(v_changed, k);
            v_old_diff := v_old_diff || jsonb_build_object(k, v_old -> k);
            v_new_diff := v_new_diff || jsonb_build_object(k, v_new -> k);
        END IF;
    END LOOP;

    -- Nothing actually changed - don't log noise.
    IF array_length(v_changed, 1) IS NULL THEN
        RETURN NULL;
    END IF;

    INSERT INTO metadata.transaction_audit
        (transaction_id, transaction_hash, change_type,
         changed_fields, old_values, new_values, changed_by, change_reason)
    VALUES
        (NEW.transaction_id, NEW.transaction_hash, 'UPDATE',
         v_changed, v_old_diff, v_new_diff, v_actor, v_reason);
    RETURN NULL;
END;
$$;

DROP TRIGGER IF EXISTS trg_audit_transaction ON silver.transactions;
CREATE TRIGGER trg_audit_transaction
    AFTER UPDATE OR DELETE ON silver.transactions
    FOR EACH ROW
    EXECUTE FUNCTION metadata.fn_audit_transaction_change();

-- -----------------------------------------------------------------------------
-- 8. Grants - replace :admin_role with your Postgres role before running
-- -----------------------------------------------------------------------------
GRANT ALL PRIVILEGES ON ALL TABLES    IN SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
GRANT USAGE                           ON SCHEMA staging,  bronze, silver, gold, metadata TO :admin_role;
