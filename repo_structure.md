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
├── init_scripts/                      # Database initialization
│   └── 01_create_tables.sql          # Creates expense tables on first run
│
├── src/                               # Source code
│   ├── __init__.py
│   ├── pipeline.py                   # Main pipeline orchestration
│   ├── extractors/                   # Data extraction modules
│   │   ├── __init__.py
│   │   └── google_drive_extractor.py
│   ├── transformers/                 # Data transformation logic
│   │   ├── __init__.py
│   │   ├── expense_transformer.py    # Main transformation logic
│   │   └── data_quality.py           # Validation checks
│   ├── loaders/                      # Data loading modules
│   │   ├── __init__.py
│   │   ├── postgres_loader.py        # PostgreSQL operations
│   │   └── minio_archiver.py         # MinIO archiving
│   └── utils/                        # Utility functions
│       ├── __init__.py
│       ├── config.py                 # Configuration management
│       └── logger.py                 # Logging setup
│
├── data/                             # Local data directory (gitignored)
│   ├── raw/                          # Downloaded CSVs from Google Drive
│   ├── processed/                    # Transformed data (temporary)
│   └── archive/                      # Local backup of processed files
│
├── logs/                             # Application logs (gitignored)
│   ├── postgres/                     # PostgreSQL logs
│   └── pipeline/                     # Pipeline execution logs
│
├── notebooks/                        # Jupyter notebooks for analysis
│   └── data_exploration.ipynb        # Initial data exploration
│
├── tests/                            # Unit tests (future)
│   ├── __init__.py
│   └── test_transformers.py
│
├── scripts/                          # Utility scripts
│   ├── run_pipeline.py               # Main entry point (manual trigger)
│   ├── setup_minio_buckets.py        # Creates MinIO buckets on first run
│   └── verify_setup.py               # Validates infrastructure
│
├── requirements.txt                  # Python dependencies
└── data_check_scripts/               # Your existing analysis scripts
    └── expense_analysis.py           # Keep for reference
```

## Key Files Purpose

### Core Pipeline Files
- **`scripts/run_pipeline.py`**: The main script you'll run manually each month
- **`src/pipeline.py`**: Orchestrates the ETL process
- **`src/transformers/expense_transformer.py`**: Your improved expense_analysis.py logic

### Infrastructure Files
- **`docker-compose.yml`**: Defines PostgreSQL + MinIO containers
- **`init_scripts/01_create_tables.sql`**: Auto-creates database schema on first run
- **`.env`**: Stores passwords and credentials (NEVER commit this!)

### Data Flow Directories
- **`data/raw/`**: Downloaded CSVs from Google Drive land here
- **`data/processed/`**: Temporary staging during transformation
- **`data/archive/`**: Local backup after successful processing
- MinIO `archive/` bucket: Cloud-style archive of all processed files
