# 📈 Market Intelligence Data Platform

> An end-to-end data engineering portfolio project — ingesting real financial market data through a medallion lakehouse architecture, modeled with dbt, and served via a live Streamlit dashboard.

[![Live Demo](https://static.streamlit.io/badges/streamlit_badge_black_white.svg)](https://market-intelligence-platform-i9bxnemqbmmk8kn7yfxxpb.streamlit.app)

---

## 🗺️ Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DATA SOURCES                                    │
│   Yahoo Finance API         FRED Macro Indicators                       │
│   (OHLCV, 14 tickers)       (Fed Funds Rate, CPI, GDP, Unemployment)    │
└───────────────┬─────────────────────────┬──────────────────────────────┘
                │                         │
                ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        INGESTION LAYER                                  │
│                     Python Ingestion Scripts                             │
│              ingestion_script.py · fred_macro.py                        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                         LAKEHOUSE — Local / DuckDB                      │
│                                                                         │
│  🥉 BRONZE (Raw)         🥈 SILVER (Cleaned)      🥇 GOLD (dbt models)  │
│  ─────────────────        ──────────────────       ─────────────────── │
│  Raw CSV as-is            Schema enforced          mrt_daily_returns    │
│  Append-only              Deduped & typed          mrt_moving_averages  │
│  Date-stamped files       Null validation          mrt_sector_perf      │
│                           Rejection tagging        mrt_macro_overlay    │
│                           Quarantine table                               │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                        TRANSFORMATION LAYER                             │
│                    dbt (models, tests, documentation)                   │
│              Staging → Marts · Full lineage graph                        │
└───────────────────────────────┬─────────────────────────────────────────┘
                                │
                                ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                          SERVING LAYER                                  │
│                     Streamlit + Plotly Dashboard                        │
│        Stock trends · Moving averages · Sector heatmap · Macro overlay  │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 🧰 Tech Stack

| Layer | Tool |
|---|---|
| Ingestion | Python, `yfinance`, `fredapi` |
| Storage | Local CSV (Bronze/Silver), DuckDB (Gold) |
| Transformation (Silver) | pandas — schema enforcement, dedup, quarantine |
| Transformation (Gold) | dbt + DuckDB |
| Dashboard | Streamlit + Plotly |
| Version Control | Git + GitHub |

---

## 📁 Project Structure

```
market-intelligence-platform/
│
├── ingestion/                    # Python ingestion scripts
│   ├── ingestion_script.py       # Yahoo Finance — OHLCV price data
│   └── fred_macro.py             # FRED — macro indicators
│
├── spark/                        # Transformation scripts
│   └── bronze_to_silver.py       # Bronze → Silver cleaning pipeline
│
├── dbt/                          # dbt Gold layer
│   └── models/
│       ├── staging/              # stg_yahoo_prices, stg_fred_macro
│       └── marts/                # mrt_daily_returns, mrt_moving_averages
│                                 # mrt_sector_performance, mrt_macro_overlay
│
├── dashboard/                    # Streamlit app
│   ├── app.py                    # Main dashboard
│   └── data/                     # CSV exports of Gold layer (for deployment)
│
├── docs/                         # Architecture diagrams, screenshots
├── .env                          # API keys (not committed)
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🔄 Data Pipeline Detail

### 1. Ingestion (Bronze Layer)
- **Yahoo Finance** — Daily OHLCV data for 14 tickers across 4 sectors (Tech, Finance, Healthcare, Energy/Consumer). 2 years of history, ~7,000 rows per run.
- **FRED API** — 5 macro indicators: Federal Funds Rate, CPI, 10Y Treasury Yield, Unemployment Rate, GDP. Monthly/quarterly frequency.
- Output: date-stamped CSVs in `data/bronze/` — append-only, never overwritten

### 2. Silver Layer (Cleaned)
- Timezone stripping on Yahoo Finance dates
- Explicit type casting for all numeric columns
- Field-level validation engine — invalid records routed to `data/quarantine/` with rejection reason tags
- Two-tier deduplication: within-file + cross-file on ticker + date
- Silver metadata columns added: `silver_processed_date`, `silver_version`

### 3. Gold Layer (dbt models)

| Model | Description |
|---|---|
| `stg_yahoo_prices` | Staging view — clean column names and types |
| `stg_fred_macro` | Staging view — macro indicators |
| `mrt_daily_returns` | Daily % return per ticker using LAG window function |
| `mrt_moving_averages` | 7d, 30d, 90d moving averages + 30d annualized volatility |
| `mrt_sector_performance` | Aggregated returns by sector per day |
| `mrt_macro_overlay` | Stock prices joined to macro indicators via ASOF join |

### 4. Dashboard
- **Live URL:** https://market-intelligence-platform-i9bxnemqbmmk8kn7yfxxpb.streamlit.app
- KPI metrics — Avg Daily Return, Best/Worst Performer, Fed Funds Rate
- Price trends with 30d moving average overlays
- Daily returns and 30d annualized volatility charts
- Sector performance heatmap (green/red)
- Macro overlay — Fed Funds Rate vs stock price on dual axis
- Sidebar filters — ticker selector + date range picker
- Raw data explorer (expandable)

---

## ✅ Data Quality Strategy

| Check | Layer | Method |
|---|---|---|
| Schema enforcement | Silver | pandas type casting |
| Null checks | Silver | Validation engine |
| Range validation | Silver | Per-indicator bounds (e.g. CPI 100–500) |
| Duplicate prevention | Silver | `drop_duplicates` on ticker + date |
| Rejection tagging | Silver | Quarantine table with reason column |
| Referential integrity | Gold | dbt `relationships` tests |
| Freshness checks | Gold | dbt `source freshness` |

---

## 🚀 Getting Started

### Prerequisites
- Python 3.11+
- Git

### Setup
```bash
git clone https://github.com/andresemejia/market-intelligence-platform
cd market-intelligence-platform

# Create virtual environment
python3.11 -m venv dbt-env
source dbt-env/bin/activate

# Install dependencies
pip install -r requirements.txt

# Add your API keys
cp .env.example .env
# Edit .env and add your FRED_API_KEY
```

### Run the Pipeline
```bash
# Step 1 — Ingest Bronze data
python3 ingestion/ingestion_script.py
python3 ingestion/fred_macro.py

# Step 2 — Transform to Silver
python3 spark/bronze_to_silver.py

# Step 3 — Build Gold layer
cd dbt && dbt run && cd ..

# Step 4 — Launch dashboard
streamlit run dashboard/app.py
```

---

## 🗓️ Build Roadmap

- [x] **Phase 1** — Yahoo Finance + FRED ingestion scripts ✅
- [x] **Phase 2** — Bronze → Silver transformation pipeline ✅
- [x] **Phase 3** — dbt Gold layer (4 mart models) ✅
- [x] **Phase 4** — Streamlit dashboard deployed publicly ✅
- [ ] **Phase 5** — SEC EDGAR ingestion (fundamentals)
- [ ] **Phase 6** — Airflow + Docker orchestration
- [ ] **Phase 7** — Migrate to Azure (ADLS + Databricks + PySpark)

---

## 👤 Author

**Andres Mejia**
[LinkedIn](https://linkedin.com/in/andres-mejia-7381a916) · [GitHub](https://github.com/andresemejia)
