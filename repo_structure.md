# Personal Finance Pipeline - Repository Structure

```
personal-finance-pipeline/
├── README.md                          # Main documentation
├── docker-compose.yml                 # Infrastructure definition
├── .env                               # Environment variables (DO NOT COMMIT)
├── .env.template                      # Template for .env
├── .gitignore                         # Git ignore file
│
├── config/                            # Configuration files
│   └── postgres/
│       └── postgres.conf              # PostgreSQL config
│
├── init_scripts/                      # Database initialization (Docker mount)
│   └── *.sql                          # Numbered SQL scripts for schema creation
│
├── scripts/                           # CLI entry points and utility scripts
│   ├── sql/                           # Recovery and maintenance SQL
│   │   └── recover_clean_state.sql   # Reset staging, bronze, silver to clean state
│   ├── run_pipeline.py                # Main entry point (full/incremental, api/file)
│   ├── inspect_wallet_export.py       # Inspect Wallet export files
│   ├── inspect_api_output.py          # Inspect API data at each stage
│   ├── inspect_incremental_load.py    # Dry-run incremental pipeline
│   ├── compare_datasets.py            # Compare two expense datasets
│   ├── deep_analysis.py               # Deeper comparison for missing transactions
│   └── cleanup.ps1                   # Repo cleanup helper
│
├── src/                               # Source code
│   ├── __init__.py
│   ├── extractors/                    # Data extraction
│   │   ├── __init__.py
│   │   ├── budgetbakers_extractor.py  # BudgetBakers REST API
│   │   └── api_field_mapper.py        # Map API fields to transformer schema
│   ├── transformers/                  # Data transformation
│   │   ├── __init__.py
│   │   └── expense_transformer.py     # Clean, enrich, hash transactions
│   ├── loaders/                       # Database loading
│   │   ├── __init__.py
│   │   ├── initial_load.py            # Full historical load (truncate silver)
│   │   ├── incremental_load.py        # Append-only load with hash dedup
│   │   └── duplicates.py              # Export duplicate hashes to CSV
│   └── utils/                         # Utilities
│       ├── __init__.py
│       ├── db_connector.py            # PostgreSQL connection helper
│       └── hash_generator.py          # Transaction hash for deduplication
│
├── flows/                             # Prefect orchestration
│   └── expense_pipeline_flow.py      # Scheduled or manual pipeline runs
│
├── data/                              # Local data (gitignored)
│   ├── raw/                           # Wallet app exports (CSV/XLSX)
│   ├── processed/                     # Temporary API extracts
│   └── inspection/                   # Pipeline inspection outputs
│
├── tests/                             # Unit tests
│   ├── test_api_field_mapper.py
│   └── test_flatten_record.py
│
├── data_check_scripts/                # Standalone analysis scripts
│   └── expense_analysis.py           # Dataset comparison and analysis
│
├── architecture_doc.md               # Architecture and design
├── repo_structure.md                  # This file
└── requirements.txt                   # Python dependencies
```

## Key Files Purpose

### Core Pipeline
- **`scripts/run_pipeline.py`**: Main CLI entry point. Run full or incremental load from API or file.
- **`src/loaders/initial_load.py`**: Full load (truncate silver, load from file).
- **`src/loaders/incremental_load.py`**: Incremental load (append only, hash-based dedup).
- **`src/transformers/expense_transformer.py`**: Cleans and enriches raw data for loading.

### Infrastructure
- **`docker-compose.yml`**: PostgreSQL (and optional MinIO) containers.
- **`init_scripts/`**: SQL scripts for schema creation (staging, bronze, silver, metadata).
- **`.env`**: Credentials (NEVER commit).

### Data Flow
- **`data/raw/`**: Source exports from Wallet app.
- **`data/processed/`**: Temporary files during API-based full load.
- **`data/inspection/`**: Duplicate exports, inspect script outputs.
