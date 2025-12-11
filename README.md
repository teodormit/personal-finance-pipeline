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

```
Wallet App (CSV Export) → Google Drive → Manual Trigger → MinIO Archive → 
Transform (Python) → PostgreSQL → Tableau Public Dashboard
```

## 🏗️ Architecture

### Technology Stack
- **Data Warehouse**: PostgreSQL 17 (self-hosted via Docker)
- **Object Storage**: MinIO (S3-compatible, for archiving & learning)
- **Transformation**: Python 3.11+ with Pandas
- **Orchestration**: Manual trigger (Phase 1), Prefect planned (Phase 2)
- **Visualization**: Tableau Public
- **Infrastructure**: Docker Compose

### Key Features
- ✅ Delta processing (only new transactions inserted)
- ✅ Data quality validation (date parsing, null checks)
- ✅ Historical archiving in MinIO
- ✅ Idempotent pipeline (safe to re-run)
- ✅ Comprehensive logging

## 🚀 Quick Start

### Prerequisites
- Docker & Docker Compose installed
- Python 3.11+
- Google Drive account (for CSV storage)
- Tableau Public Desktop (for visualization)

### Monthly Workflow

1. **Export data from Wallet app**
   - Open Wallet (BudgetBakers) app
   - Navigate to: Settings → Export Data → CSV
   - Save to Google Drive: `/Finance/Wallet_Exports/`

2. **Download CSV to local machine**
   - Place file in: `data/raw/expense_export_YYYY-MM-DD.csv`

3. **Run the pipeline**
   ```bash
   python scripts/run_pipeline.py
   ```

4. **Refresh Tableau dashboard**
   - Open Tableau Public Desktop
   - Refresh data extract
   - Publish updated dashboard
