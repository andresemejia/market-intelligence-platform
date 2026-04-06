# =============================================================================
# SECTION 1: IMPORTS
# These are the libraries we need. You install them once via pip, then import
# them at the top of every script that uses them.
# =============================================================================

import yfinance as yf         # Pulls stock data from Yahoo Finance
import pandas as pd           # Handles data in table format (DataFrames)
import os                     # Lets us create folders and work with file paths
import logging                # Prints status so we know what's happening
from datetime import datetime # Used to timestamp our files and logs

# =============================================================================
# SECTION 2: LOGGING SETUP
# Logging is like print() but smarter. It adds timestamps, severity levels
# (INFO, WARNING, ERROR), and can be saved to a file. This is standard practice
# in data engineering — you always want a record of what your pipeline did.
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# SECTION 3: CONFIGURATION
# All the settings live here at the top so they're easy to find and change.
# If you want to add a new ticker or change the date range, you only need to
# edit this section — not hunt through the whole script.
# =============================================================================

# The stocks we want to track. These are ticker symbols:
# AAPL = Apple, MSFT = Microsoft, GOOGL = Google, AMZN = Amazon, META = Meta
# JPM = JPMorgan, GS = Goldman Sachs (financial sector for variety)

TICKERS = [
    "AAPL", "MSFT", "NVDA",        # Tech
    "JPM", "GS", "BAC",            # Finance
    "JNJ", "PFE", "UNH",           # Healthcare
    "XOM", "CVX",                  # Energy
    "AMZN", "WMT", "COST",         # Consumer
]

# How far back we want data. "2y" = 2 years. Options: 1d, 5d, 1mo, 3mo,
# 6mo, 1y, 2y, 5y, 10y, ytd, max
PERIOD = "2y"

# Where the raw data will be saved - this is the Bronze Layer folder
OUTPUT_DIR = "data/bronze/yahoo_finance"

# Today's date - used to name or output file so each run creates a new file
# and we never overwrite old data (append-only Bronze layer principle)
TODAY = datetime.today().strftime("%Y-%m-%d")

# =============================================================================
# SECTION 4: HELPER FUNCTION — CREATE OUTPUT FOLDER
# A small utility function that creates the output folder if it doesn't exist.
# os.makedirs with exist_ok=True means: create the folder, but don't crash
# if it's already there.
# =============================================================================

def create_output_dir(path: str) -> None:
    """Creates the output directory if it doesn't exsist already"""
    os.makedirs(path, exist_ok=True)
    logger.info(f"Output directory ready: {path}")

# =============================================================================
# SECTION 5: CORE FUNCTION — FETCH STOCK DATA
# This is the main logic. It loops through each ticker, downloads the price
# history, adds metadata columns, and returns everything as one combined table.
# =============================================================================

def fetch_stock_data(tickers: list, period: str) -> pd.DataFrame:
    """
    Downloads historical OHLCV data for list of tickers.

    OHLCV = Open, High, Low, Volume - the standard columns in any
    stock price dataset.

    Returns a pandas DataFrame with all tickers combined.
    """
    all_data = [] # We'll collect each ticker's data, then combine

    for ticker in tickers:
        logger.info(f"Fetching data for {ticker}...")

        try:
            # yf.Ticker() creates a ticker object. .history() pulls the data
            import requests
            session = requests.Session()
            session.headers.update({
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
            })
            stock = yf.Ticker(ticker, session=session)
            df = stock.history(period=period)

            # If nothing came back (e.g. invalid ticker), skip it
            if df.empty:
                logger.warning(f"No data returned for {ticker} - skipping.")
                continue

            # --- ADD META DATA COLUMNS ---
            # These columns don't come from Yahoo Finance - we add them 
            # ourselves to make data more useful downstream.

            df["ticker"] = ticker                # Which stock is this row?
            df["ingestion_date"] = TODAY         # When did we pull this
            df["source"] = "yahoo_finance"       # Where did it come from?

            # Reset index so 'Date' becomes a regular column (not the index)
            df = df.reset_index()

            # Rename columns to snake_case - cleaner for SQL and dbt later
            df.columns = [c.lower().replace(" ", "_") for c in df.columns]

            all_data.append(df)
            logger.info(f"  -> {len(df)} rows fetched for {ticker}")

        except Exception as e:
            # If one ticker fails, log the error but keep going for the rest.
            # We don't want one bad ticker to kill the whole pipeline.
            logger.error(f"Failed to fetch {ticker}: {e}")
            continue

    # Combine all individual ticker DataFrames into one big table
    if not all_data:
        raise ValueError("No data was feteched for any ticker. Check your ticker list")
    
    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined

# =============================================================================
# SECTION 6: CORE FUNCTION — SAVE TO BRONZE LAYER
# Saves the data as a CSV file in the Bronze folder. The filename includes
# today's date so every run creates a new file — this is the append-only
# principle of the Bronze layer. We never modify or delete raw data.
# =============================================================================

def run_quality_checks(df: pd.DataFrame) -> bool:
    
    """
    Runs basic data quality checks on the fetched data.
    Returns True if all checks pass, False otherwise.
    """
    logger.error("Running data quality checks")

    # Check 1: Is the Dataframe empty
    if df.empty:
        logger.error("QUALITY CHECK FAILED: DataFrame is empty.")
        return False
    
    # check 2: Do all required columns exist?
    required_columns = ["date", "open", "high", "low", "close", "volume", "ticker"]
    missing = [col for col in required_columns if col not in df.columns]
    if missing:
        logger.error("QUALITY CHECK FAILED: Missing columns: {missing}")
        return False
    
    # Check 3: How many null values are in critical columns?
    null_counts = df[required_columns].isnull().sum()
    if null_counts.any():
        logger.warning(f"Null values detected:\n{null_counts[null_counts > 0]}")
        # We warn but don't fail — nulls will be handled in the Silver layer

    # Check 4: Are all close prices positive? (Basic sanity)
    if (df["close"] <= 0).any():
        logger.error("QUALITY CHECK FAILED: Some close prices are zero or negative.")
        return False
    
    logger.info(f"Quality checks passed. Shape: {df.shape}")
    return True
            
# =============================================================================
# SECTION 8: MAIN FUNCTION — ORCHESTRATES EVERYTHING
# This ties all the pieces together. In Python, wrapping your main logic in a
# main() function and calling it with `if __name__ == "__main__"` is best
# practice — it means the script only runs when you execute it directly,
# not when it's imported by another file (like Airflow will do later).
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("Starting Yahoo Finance ingestion")
    logger.info(f"Tickers: {TICKERS}")
    logger.info(f"Period: {PERIOD}")
    logger.info("=" * 60)

    # Step 1: Make sure the output folder exists
    create_output_dir(OUTPUT_DIR)

    # Step 2: Fetch the data
    df = fetch_stock_data(TICKERS, PERIOD)

    # Step 3: Run quality checks
    checks_passed = run_quality_checks(df)
    if not checks_passed:
        logger.error("Pipeline stopped due to failed quality checks.")
        return  # Stop here — don't save bad data to Bronze

    # Step 4: Save to Bronze layer
    filepath = save_to_bronze(df, OUTPUT_DIR, TODAY)

    logger.info("=" * 60)
    logger.info("Ingestion complete!")
    logger.info(f"File: {filepath}")
    logger.info(f"Rows: {len(df)}")
    logger.info(f"Tickers: {df['ticker'].nunique()} stocks")
    logger.info("=" * 60)

def save_to_bronze(df, output_dir, date):
    """Saves the DataFrame as a CSV file to the Bronze layer."""
    filename = f"yahoo_prices_{date}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    logger.info(f"Data saved to: {filepath}")
    return filepath

if __name__ == "__main__":
    main()


        
