# personal-finance-pipeline
###### An automated ETL pipeline for personal finance data.
A self-hosted, automated ETL pipeline for personal expense tracking data from Wallet (BudgetBakers) app to PostgreSQL, with Tableau Public visualization.

## Start Date 28.06.2025
- Cloned and initialized on my new local environment - New Laptop
## Project Phases 
### Environment setup  
#### PostgresSQL instance - 13.07.2025
        * Port 5432
#### MinIO setup  - 19.07.2025
        * Web console access (port 9001)
        * API endpoint (port 9000)
#### Airbyte setup
        *Separate service management via abctl
        *Accessible on port 8000

Airbyte connection has beend established to the G  

## Project 2nd Phase - 28.11.2025
- Continue with current architecture but simplify the pipeline
- Will no longer use Airbyte for loading
- BudgetBakers API extraction (Premium plan required)

```
BudgetBakers API → Transform (Python) → PostgreSQL → Tableau Public
       OR
Wallet App (CSV/XLSX Export) → Transform → PostgreSQL → Tableau Public
```

## Architecture

### Technology Stack
- **Data Warehouse**: PostgreSQL 17 (self-hosted via Docker)
- **Extraction**: BudgetBakers REST API (Premium) or file export
- **Transformation**: Python 3.11+ with Pandas
- **Orchestration**: Prefect or cron / Task Scheduler
- **Visualization**: Tableau Public
- **Infrastructure**: Docker Compose

### Key Features
- BudgetBakers API extraction (automated, no manual export)
- Incremental loading (only new transactions inserted)
- Delta processing with transaction hash deduplication
- Data quality validation (date parsing, null checks)
- Idempotent pipeline (safe to re-run)
- Comprehensive logging

## Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Python 3.11+
- BudgetBakers Wallet Premium + API token (from web.budgetbakers.com/settings/apiTokens)
- Tableau Public Desktop (for visualization)

### Setup
1. Copy `.env.template` to `.env` and set `BUDGETBAKERS_API_TOKEN`
2. Start PostgreSQL: `docker compose up -d postgres`
3. Install deps: `pip install -r requirements.txt`

### Run Pipeline

**Incremental (daily updates from API):**
```bash
python scripts/run_pipeline.py --mode incremental --source api
```

**Full load from file (initial or manual):**
```bash
python scripts/run_pipeline.py --mode full --source file --file data/raw/export.xlsx
```

**Full load from API (date range):**
```bash
python scripts/run_pipeline.py --mode full --source api --from-date 2024-01-01 --to-date 2025-01-01
```

### Scheduled Runs (Prefect)
```bash
prefect deployment build flows/expense_pipeline_flow.py:expense_pipeline_flow --cron "0 6 * * *"
prefect agent start
```

### Tableau
- Connect to `silver.v_tableau_transactions`
- Refresh data extract after each pipeline run
