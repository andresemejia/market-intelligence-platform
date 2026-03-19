# =============================================================================
# MARKET INTELLIGENCE PLATFORM — STREAMLIT DASHBOARD
# Connects directly to DuckDB (Gold layer) and visualizes:
# 1. Stock price trends with moving averages
# 2. Daily returns heatmap
# 3. Sector performance comparison
# 4. Macro overlay (Fed Funds Rate, CPI vs stock performance)
# =============================================================================

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

# =============================================================================
# SECTION 1: PAGE CONFIG
# Sets the browser tab title, icon, and layout.
# wide layout uses the full screen width — better for charts.
# =============================================================================

st.set_page_config(
    page_title="Market Intelligence Platform",
    page_icon="📈",
    layout="wide"
)

# =============================================================================
# SECTION 2: DATA LOADING
# When running on Streamlit Cloud, reads from CSV exports.
# When running locally, reads from DuckDB for better performance.
# =============================================================================

import os

DATA_DIR = "dashboard/data"
DB_PATH = "data/market_intelligence.duckdb"

@st.cache_data
def load_data():
    # Check if DuckDB file exists (local) or use CSVs (cloud)
    if os.path.exists(DB_PATH):
        import duckdb
        conn = duckdb.connect(DB_PATH, read_only=True)
        daily_returns     = conn.execute("SELECT * FROM mrt_daily_returns").df()
        moving_averages   = conn.execute("SELECT * FROM mrt_moving_averages").df()
        sector_performance = conn.execute("SELECT * FROM mrt_sector_performance").df()
        macro_overlay     = conn.execute("SELECT * FROM mrt_macro_overlay").df()
        conn.close()
    else:
        # Streamlit Cloud — read from CSV exports
        daily_returns     = pd.read_csv(f"{DATA_DIR}/mrt_daily_returns.csv")
        moving_averages   = pd.read_csv(f"{DATA_DIR}/mrt_moving_averages.csv")
        sector_performance = pd.read_csv(f"{DATA_DIR}/mrt_sector_performance.csv")
        macro_overlay     = pd.read_csv(f"{DATA_DIR}/mrt_macro_overlay.csv")

    return daily_returns, moving_averages, sector_performance, macro_overlay

df_returns, df_ma, df_sector, df_macro = load_data()


# =============================================================================
# SECTION 4: HEADER
# =============================================================================

st.title("📈 Market Intelligence Platform")
st.markdown("End-to-end financial data pipeline · Yahoo Finance + FRED Macro · Built with Python, dbt, DuckDB")
st.divider()

# =============================================================================
# SECTION 5: SIDEBAR FILTERS
# Lets the user filter by ticker and date range.
# All charts below will respond to these filters.
# =============================================================================

st.sidebar.header("Filters")

# Ticker selector
all_tickers = sorted(df_returns["ticker"].unique().tolist())
selected_tickers = st.sidebar.multiselect(
    "Select Tickers",
    options=all_tickers,
    default=["AAPL", "MSFT", "NVDA"]
)

# Date range selector
min_date = pd.to_datetime(df_returns["price_date"]).min().date()
max_date = pd.to_datetime(df_returns["price_date"]).max().date()
default_start = max_date - timedelta(days=180)

date_range = st.sidebar.date_input(
    "Date Range",
    value=(default_start, max_date),
    min_value=min_date,
    max_value=max_date
)

# Handle date range unpacking safely
if isinstance(date_range, (list, tuple)) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date, end_date = default_start, max_date

# Filter DataFrames based on selections
df_ma_filtered = df_ma[
    (df_ma["ticker"].isin(selected_tickers)) &
    (pd.to_datetime(df_ma["price_date"]).dt.date >= start_date) &
    (pd.to_datetime(df_ma["price_date"]).dt.date <= end_date)
]

df_returns_filtered = df_returns[
    (df_returns["ticker"].isin(selected_tickers)) &
    (pd.to_datetime(df_returns["price_date"]).dt.date >= start_date) &
    (pd.to_datetime(df_returns["price_date"]).dt.date <= end_date)
]

df_sector_filtered = df_sector[
    (pd.to_datetime(df_sector["price_date"]).dt.date >= start_date) &
    (pd.to_datetime(df_sector["price_date"]).dt.date <= end_date)
]

df_macro_filtered = df_macro[
    (df_macro["ticker"].isin(selected_tickers)) &
    (pd.to_datetime(df_macro["price_date"]).dt.date >= start_date) &
    (pd.to_datetime(df_macro["price_date"]).dt.date <= end_date)
]

# =============================================================================
# SECTION 6: KPI METRICS ROW
# Shows key numbers at the top of the dashboard.
# st.columns splits the page into side-by-side panels.
# =============================================================================

st.subheader("📊 Key Metrics")

if not df_returns_filtered.empty and selected_tickers:
    col1, col2, col3, col4 = st.columns(4)

    # Average daily return across selected tickers
    avg_return = df_returns_filtered["daily_return_pct"].mean()
    col1.metric("Avg Daily Return", f"{avg_return:.3f}%")

    # Best performing ticker
    best = df_returns_filtered.groupby("ticker")["daily_return_pct"].mean().idxmax()
    best_val = df_returns_filtered.groupby("ticker")["daily_return_pct"].mean().max()
    col2.metric("Best Performer", best, f"{best_val:.2f}%")

    # Worst performing ticker
    worst = df_returns_filtered.groupby("ticker")["daily_return_pct"].mean().idxmin()
    worst_val = df_returns_filtered.groupby("ticker")["daily_return_pct"].mean().min()
    col3.metric("Worst Performer", worst, f"{worst_val:.2f}%")

    # Current Fed Funds Rate
    latest_macro = df_macro_filtered.dropna(subset=["fed_funds_rate"])
    if not latest_macro.empty:
        fed_rate = latest_macro["fed_funds_rate"].iloc[-1]
        col4.metric("Fed Funds Rate", f"{fed_rate:.2f}%")
    else:
        col4.metric("Fed Funds Rate", "N/A")

st.divider()

# =============================================================================
# SECTION 7: PRICE CHART WITH MOVING AVERAGES
# Line chart showing close price + 30d moving average per ticker.
# =============================================================================

st.subheader("📈 Price Trends with Moving Averages")

if not df_ma_filtered.empty:
    fig_price = go.Figure()

    for ticker in selected_tickers:
        ticker_data = df_ma_filtered[df_ma_filtered["ticker"] == ticker]

        # Close price line
        fig_price.add_trace(go.Scatter(
            x=ticker_data["price_date"],
            y=ticker_data["close_price"],
            name=f"{ticker} Price",
            mode="lines",
            line=dict(width=1.5)
        ))

        # 30d moving average line (dashed)
        fig_price.add_trace(go.Scatter(
            x=ticker_data["price_date"],
            y=ticker_data["ma_30d"],
            name=f"{ticker} MA30",
            mode="lines",
            line=dict(width=1, dash="dash")
        ))

    fig_price.update_layout(
        height=450,
        hovermode="x unified",
        legend=dict(orientation="h", yanchor="bottom", y=1.02),
        xaxis_title="Date",
        yaxis_title="Price (USD)"
    )
    st.plotly_chart(fig_price, use_container_width=True)
else:
    st.info("No data available for selected filters.")

st.divider()

# =============================================================================
# SECTION 8: DAILY RETURNS + VOLATILITY
# Two charts side by side:
# Left  — daily return % over time
# Right — 30d rolling volatility
# =============================================================================

st.subheader("📉 Daily Returns & Volatility")

col_left, col_right = st.columns(2)

with col_left:
    if not df_returns_filtered.empty:
        fig_returns = px.line(
            df_returns_filtered,
            x="price_date",
            y="daily_return_pct",
            color="ticker",
            title="Daily Return %",
            labels={"daily_return_pct": "Return %", "price_date": "Date"}
        )
        fig_returns.update_layout(height=350)
        st.plotly_chart(fig_returns, use_container_width=True)

with col_right:
    if not df_ma_filtered.empty:
        fig_vol = px.line(
            df_ma_filtered,
            x="price_date",
            y="volatility_30d_annualized",
            color="ticker",
            title="30d Annualized Volatility",
            labels={"volatility_30d_annualized": "Volatility", "price_date": "Date"}
        )
        fig_vol.update_layout(height=350)
        st.plotly_chart(fig_vol, use_container_width=True)

st.divider()

# =============================================================================
# SECTION 9: SECTOR PERFORMANCE HEATMAP
# Shows average daily return per sector over time as a heatmap.
# Green = positive returns, Red = negative returns.
# =============================================================================

st.subheader("🗺️ Sector Performance")

if not df_sector_filtered.empty:
    # Pivot for heatmap: rows = sectors, columns = dates
    sector_pivot = df_sector_filtered.pivot_table(
        index="sector",
        columns="price_date",
        values="avg_daily_return_pct"
    )

    fig_heatmap = px.imshow(
        sector_pivot,
        color_continuous_scale="RdYlGn",
        color_continuous_midpoint=0,
        title="Sector Average Daily Return % (Green=Positive, Red=Negative)",
        aspect="auto"
    )
    fig_heatmap.update_layout(height=300)
    st.plotly_chart(fig_heatmap, use_container_width=True)

st.divider()

# =============================================================================
# SECTION 10: MACRO OVERLAY
# Shows Fed Funds Rate over time with stock performance overlaid.
# Helps visualize the relationship between monetary policy and markets.
# =============================================================================

st.subheader("🏦 Macro Overlay — Fed Funds Rate vs Stock Performance")

if not df_macro_filtered.empty and selected_tickers:
    # Pick first selected ticker for overlay
    overlay_ticker = selected_tickers[0]
    macro_ticker = df_macro_filtered[
        df_macro_filtered["ticker"] == overlay_ticker
    ].dropna(subset=["fed_funds_rate"])

    if not macro_ticker.empty:
        fig_macro = go.Figure()

        # Stock price on primary y-axis
        fig_macro.add_trace(go.Scatter(
            x=macro_ticker["price_date"],
            y=macro_ticker["close_price"],
            name=f"{overlay_ticker} Price",
            line=dict(color="royalblue")
        ))

        # Fed Funds Rate on secondary y-axis
        fig_macro.add_trace(go.Scatter(
            x=macro_ticker["price_date"],
            y=macro_ticker["fed_funds_rate"],
            name="Fed Funds Rate %",
            line=dict(color="firebrick", dash="dash"),
            yaxis="y2"
        ))

        fig_macro.update_layout(
            height=400,
            yaxis=dict(title=f"{overlay_ticker} Price (USD)"),
            yaxis2=dict(
                title="Fed Funds Rate %",
                overlaying="y",
                side="right"
            ),
            hovermode="x unified",
            legend=dict(orientation="h", yanchor="bottom", y=1.02)
        )
        st.plotly_chart(fig_macro, use_container_width=True)

st.divider()

# =============================================================================
# SECTION 11: RAW DATA TABLE
# Lets users explore the underlying data directly in the dashboard.
# Expandable so it doesn't dominate the page.
# =============================================================================

with st.expander("🔍 View Raw Data"):
    tab1, tab2, tab3 = st.tabs(["Daily Returns", "Moving Averages", "Sector Performance"])

    with tab1:
        st.dataframe(df_returns_filtered, use_container_width=True)

    with tab2:
        st.dataframe(df_ma_filtered, use_container_width=True)

    with tab3:
        st.dataframe(df_sector_filtered, use_container_width=True)

# =============================================================================
# SECTION 12: FOOTER
# =============================================================================

st.markdown("---")
st.markdown("Built by **Andres Mejia** · Market Intelligence Platform · [GitHub](https://github.com/andresemejia)")