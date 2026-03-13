# =============================================================================
# SECTION 1: IMPORTS
# Same pattern as yahoo_finance.py. One new library here:
# - fredapi: official Python wrapper for the FRED API
# - dotenv: loads your API key from the .env file so it's never hardcoded
# =============================================================================

import pandas as pd
import os
import logging
from datetime import datetime
from fredapi import Fred
from dotenv import load_dotenv


# =============================================================================
# SECTION 2: LOGGING SETUP
# Identical to the Yahoo Finance script — consistent logging across all
# ingestion scripts makes debugging much easier later.
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)


# =============================================================================
# SECTION 3: CONFIGURATION
# INDICATORS is a dictionary mapping FRED series IDs to human-readable names.
# A FRED series ID is just the code FRED uses to identify each dataset.
# You can find more series IDs by searching at fred.stlouisfed.org
#
# The ones we're pulling:
# - FEDFUNDS : Federal Funds Rate — the interest rate the Fed sets
# - CPIAUCSL : Consumer Price Index — measures inflation
# - GS10     : 10-Year Treasury Yield — benchmark for long-term interest rates
# - UNRATE   : Unemployment Rate — % of labor force unemployed
# - GDP      : Gross Domestic Product — total US economic output (quarterly)
# =============================================================================

load_dotenv()  # This reads your .env file and loads FRED_API_KEY into memory

FRED_API_KEY = os.getenv("FRED_API_KEY")  # Fetches the key from environment

INDICATORS = {
    "FEDFUNDS": "federal_funds_rate",
    "CPIAUCSL": "consumer_price_index",
    "GS10":     "treasury_yield_10y",
    "UNRATE":   "unemployment_rate",
    "GDP":      "gross_domestic_product",
}

# How far back to pull data — matches our Yahoo Finance 2-year window
START_DATE = "2024-01-01"
END_DATE   = datetime.today().strftime("%Y-%m-%d")

# Bronze layer output folder
OUTPUT_DIR = "data/bronze/fred_macro"

# Today's date for the filename
TODAY = datetime.today().strftime("%Y-%m-%d")


# =============================================================================
# SECTION 4: CREATE OUTPUT FOLDER
# Same helper as the Yahoo Finance script.
# =============================================================================

def create_output_dir(path: str) -> None:
    """Creates the output directory if it doesn't already exist."""
    os.makedirs(path, exist_ok=True)
    logger.info(f"Output directory ready: {path}")


# =============================================================================
# SECTION 5: FETCH MACRO DATA
# This loops through each indicator, pulls the time series from FRED,
# and adds metadata columns — same pattern as fetch_stock_data().
#
# Key difference from Yahoo Finance: FRED returns a pandas Series (one column)
# not a DataFrame, so we convert it with .reset_index() and rename columns.
#
# Also note: GDP is quarterly so it will have far fewer rows than monthly
# series like CPI or FEDFUNDS. That's normal.
# =============================================================================

def fetch_macro_data(fred: Fred, indicators: dict, start: str, end: str) -> pd.DataFrame:
    """
    Downloads time series data for each macro indicator from FRED.
    Returns a single combined DataFrame in long format.

    Long format means each row is one indicator on one date:
    date        | series_id | series_name          | value  | ...
    2024-01-01  | FEDFUNDS  | federal_funds_rate   | 5.33   | ...
    2024-02-01  | FEDFUNDS  | federal_funds_rate   | 5.33   | ...
    2024-01-01  | CPIAUCSL  | consumer_price_index | 308.4  | ...
    """
    all_data = []

    for series_id, series_name in indicators.items():
        logger.info(f"Fetching {series_id} ({series_name})...")

        try:
            # fred.get_series() returns a pandas Series indexed by date
            series = fred.get_series(
                series_id,
                observation_start=start,
                observation_end=end
            )

            if series.empty:
                logger.warning(f"No data returned for {series_id} — skipping.")
                continue

            # Convert Series to DataFrame and clean up column names
            df = series.reset_index()
            df.columns = ["date", "value"]

            # Add metadata columns
            df["series_id"]    = series_id
            df["series_name"]  = series_name
            df["ingestion_date"] = TODAY
            df["source"]       = "fred"

            # Drop rows where value is null (FRED sometimes has gaps)
            null_count = df["value"].isnull().sum()
            if null_count > 0:
                logger.warning(f"  -> {null_count} null values dropped for {series_id}")
                df = df.dropna(subset=["value"])

            all_data.append(df)
            logger.info(f"  -> {len(df)} rows fetched for {series_id}")

        except Exception as e:
            logger.error(f"Failed to fetch {series_id}: {e}")
            continue

    if not all_data:
        raise ValueError("No data was fetched for any indicator. Check your API key and series IDs.")

    combined = pd.concat(all_data, ignore_index=True)
    logger.info(f"Total rows fetched: {len(combined)}")
    return combined


# =============================================================================
# SECTION 6: DATA QUALITY CHECKS
# Same pattern as Yahoo Finance but adapted for macro data.
# The key check here is that values are numeric and within reasonable ranges.
# =============================================================================

def run_quality_checks(df: pd.DataFrame) -> bool:
    """Runs basic data quality checks on the fetched macro data."""
    logger.info("Running data quality checks...")

    # Check 1: Is the DataFrame empty?
    if df.empty:
        logger.error("QUALITY CHECK FAILED: DataFrame is empty.")
        return False

    # Check 2: Required columns present?
    required_columns = ["date", "value", "series_id", "series_name"]
    missing = [c for c in required_columns if c not in df.columns]
    if missing:
        logger.error(f"QUALITY CHECK FAILED: Missing columns: {missing}")
        return False

    # Check 3: Did we get all expected indicators?
    fetched_series = df["series_id"].unique().tolist()
    expected_series = list(INDICATORS.keys())
    missing_series = [s for s in expected_series if s not in fetched_series]
    if missing_series:
        logger.warning(f"Missing series (may have been skipped): {missing_series}")

    # Check 4: Any remaining nulls in value column?
    null_count = df["value"].isnull().sum()
    if null_count > 0:
        logger.error(f"QUALITY CHECK FAILED: {null_count} null values remain.")
        return False

    # Log a summary per indicator
    summary = df.groupby("series_id")["value"].agg(["count", "min", "max"])
    logger.info(f"Summary by indicator:\n{summary.to_string()}")

    logger.info(f"Quality checks passed. Shape: {df.shape}")
    return True


# =============================================================================
# SECTION 7: SAVE TO BRONZE LAYER
# Same as Yahoo Finance — date-stamped CSV, append-only, never overwrite.
# =============================================================================

def save_to_bronze(df: pd.DataFrame, output_dir: str, date: str) -> str:
    """Saves the DataFrame as a CSV file to the Bronze layer."""
    filename = f"fred_macro_{date}.csv"
    filepath = os.path.join(output_dir, filename)
    df.to_csv(filepath, index=False)
    logger.info(f"Data saved to: {filepath}")
    return filepath


# =============================================================================
# SECTION 8: MAIN FUNCTION
# Same orchestration pattern as Yahoo Finance:
# setup → fetch → quality check → save
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("Starting FRED macro ingestion")
    logger.info(f"Indicators: {list(INDICATORS.keys())}")
    logger.info(f"Date range: {START_DATE} to {END_DATE}")
    logger.info("=" * 60)

    # Validate API key loaded correctly
    if not FRED_API_KEY:
        logger.error("FRED_API_KEY not found. Make sure your .env file exists and contains FRED_API_KEY.")
        return

    # Initialize the FRED client with our API key
    fred = Fred(api_key=FRED_API_KEY)

    # Step 1: Create output folder
    create_output_dir(OUTPUT_DIR)

    # Step 2: Fetch macro data
    df = fetch_macro_data(fred, INDICATORS, START_DATE, END_DATE)

    # Step 3: Quality checks
    checks_passed = run_quality_checks(df)
    if not checks_passed:
        logger.error("Pipeline stopped due to failed quality checks.")
        return

    # Step 4: Save to Bronze
    filepath = save_to_bronze(df, OUTPUT_DIR, TODAY)

    logger.info("=" * 60)
    logger.info("Ingestion complete!")
    logger.info(f"File: {filepath}")
    logger.info(f"Rows: {len(df)}")
    logger.info(f"Indicators: {df['series_id'].nunique()} series")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()