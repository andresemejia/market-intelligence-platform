# Market Intelligence Data Platform

> An end-to-end data engineering portfolio project — ingesting real financial market data through a medallion lakehouse architecture, modeled with dbt, orchestrated with Airflow, and served via an analytics dashboard.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│   Yahoo Finance API    SEC EDGAR Filings    FRED Macro Indicators        │
│   (OHLCV, tickers)     (10-K, 10-Q)         (interest rates, CPI)       │
└───────────────┬─────────────────┬──────────────────┬────────────────────┘
                │                 │                  │
                ▼                 ▼                  ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│              Python Ingestion Scripts + Apache Airflow DAGs              │
│               (Scheduled daily, retries, failure alerting)               │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                    LAKEHOUSE — Azure ADLS + Delta Lake                   │
│                                                                         │
│  🥉 BRONZE (Raw)         🥈 SILVER (Cleaned)      🥇 GOLD (Aggregated)  │
│  ─────────────────        ──────────────────       ─────────────────── │
│  Raw JSON/CSV as-is       Schema enforced          dbt models           │
│  Append-only              Deduped & typed          Moving averages      │
│  Quarantine table         Null validation          Volatility metrics   │
│  for bad records          Rejection tagging        Sector rollups       │
│                                                    P/E ratios, YTD      │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION LAYER                             │
│                    dbt (models, tests, documentation)                   │
│          Staging → Intermediate → Mart models, full lineage graph        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                  │
│              Power BI Dashboard  /  Streamlit App (TBD)                 │
│        Stock trends · Sector performance · Macro overlays · Alerts      │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, `yfinance`, `requests` (SEC/FRED APIs) |
| Orchestration | Apache Airflow (Dockerized) |
| Storage | Azure Data Lake Storage Gen2 (ADLS) |
| Processing | PySpark on Azure Databricks |
| Table Format | Delta Lake |
| Transformation | dbt (dbt-spark / dbt-databricks) |
| Containerization | Docker + Docker Compose |
| Dashboard | Power BI / Streamlit |
| Version Control | Git + GitHub |

---

## 📁 Project Structure

```
market-intelligence-platform/
│
├── ingestion/                  # Python scripts for each data source
│   ├── yahoo_finance.py        # OHLCV price data
│   ├── sec_edgar.py            # Fundamental filings
│   └── fred_macro.py           # Macro indicators
│
├── airflow/                    # Airflow DAGs + Docker setup
│   ├── docker-compose.yml
│   └── dags/
│       ├── market_data_dag.py
│       ├── sec_filings_dag.py
│       └── macro_data_dag.py
│
├── spark/                      # PySpark transformation jobs
│   ├── bronze_to_silver.py     # Cleaning, validation, dedup
│   └── silver_to_gold.py       # Pre-aggregations before dbt
│
├── dbt/                        # dbt project
│   ├── models/
│   │   ├── staging/            # Raw source models (stg_*)
│   │   ├── intermediate/       # Business logic (int_*)
│   │   └── marts/              # Final analytical tables (mrt_*)
│   ├── tests/                  # Data quality tests
│   └── docs/                   # Auto-generated dbt docs
│
├── dashboard/                  # Power BI file or Streamlit app
│
├── docs/                       # Architecture diagrams, data dictionary
│   └── architecture.png
│
├── .env.example                # Environment variable template
├── docker-compose.yml          # Full local environment
└── README.md
```

---

## 🔄 Data Pipeline Detail

### 1. Ingestion
- **Yahoo Finance** — Daily OHLCV data for a curated watchlist of tickers (S&P 500 constituents or a subset by sector)
- **SEC EDGAR** — Quarterly/annual filings parsed for key financial metrics (revenue, EPS, debt)
- **FRED API** — Macro indicators: Fed Funds Rate, CPI, 10Y Treasury Yield

### 2. Bronze Layer (Raw)
- Land raw data as-is (JSON/CSV) into ADLS partitioned by `ingestion_date`
- No transformations — append only
- Track ingestion metadata (source, timestamp, record count)

### 3. Silver Layer (Cleaned)
- PySpark job enforces schema with `from_json` / `StructType`
- Timestamp normalization and type casting
- Field-level validation engine — invalid records routed to `quarantine` Delta table with rejection tags
- Two-tier deduplication: within-batch `dropDuplicates()` + cross-batch Delta `MERGE`
- Target: **>95% data quality pass rate**

### 4. Gold Layer (dbt models)

| Model | Description |
|---|---|
| `mrt_daily_prices` | Clean OHLCV with adjusted close, daily returns |
| `mrt_moving_averages` | 7d, 30d, 90d moving averages per ticker |
| `mrt_volatility` | Rolling 30d annualized volatility |
| `mrt_sector_performance` | Aggregated returns by GICS sector |
| `mrt_fundamentals` | P/E, EV/EBITDA, debt ratios from SEC filings |
| `mrt_macro_overlay` | Macro indicators joined to market dates |

### 5. Orchestration (Airflow)
- Separate DAGs per data source, all scheduled daily at market close
- Retries with exponential backoff
- Slack/email alerts on failure
- Dependencies enforced: Silver job only runs after Bronze lands successfully

---

## Dashboard / Serving

**Key views planned:**
- Price trends with moving average overlays
- Sector heatmap (daily / weekly / monthly returns)
- Volatility tracker with macro event annotations
- Fundamental screener (P/E, margins by sector)
- Data quality monitor (pass/quarantine rates over time)

---

## Data Quality Strategy

| Check | Layer | Method |
|---|---|---|
| Schema enforcement | Silver | PySpark `StructType` |
| Null checks | Silver | Validation engine |
| Duplicate prevention | Silver | `dropDuplicates` + Delta MERGE |
| Referential integrity | Gold | dbt `relationships` tests |
| Freshness checks | Gold | dbt `source freshness` |
| Range validation | Gold | dbt custom tests (e.g. price > 0) |

---

## Getting Started

> *This section is a work in progress — setup instructions will be added as the project is built.*

### Prerequisites
- Docker + Docker Compose
- Azure account (free tier works for dev)
- Python 3.10+

### Environment Setup
```bash
git clone https://github.com/andresemejia/market-intelligence-platform
cd market-intelligence-platform
cp .env.example .env
# Fill in your API keys and Azure credentials
docker-compose up -d
```

---

## Build Roadmap

- [x] **Phase 1** — Ingestion scripts + Airflow DAGs (Yahoo Finance)
- [x] **Phase 2** — Bronze → Silver PySpark pipeline on Databricks
- [ ] **Phase 3** — dbt models (staging → marts)
- [ ] **Phase 4** — Add SEC EDGAR + FRED sources
- [ ] **Phase 5** — Dashboard (Power BI or Streamlit)
- [ ] **Phase 6** — Polish: dbt docs, data dictionary, architecture diagram

---
