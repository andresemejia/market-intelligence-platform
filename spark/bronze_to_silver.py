# =============================================================================
# SECTION 1: IMPORTS
# Same pattern as ingestion scripts. No new libraries needed —
# everything here uses pandas and os which you already have installed.
# =============================================================================


import pandas as pd
import os
import logging
from datetime import datetime
from typing import Tuple

# =============================================================================
# SECTION 2: LOGGING SETUP
# Identical setup across all scripts for consistent log formatting.
# =============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)
logger = logging.getLogger(__name__)

# =============================================================================
# SECTION 3: CONFIGURATION
# Input paths point to Bronze layer files.
# Output paths point to Silver layer folders.
# Quarantine folder is where bad records go instead of being silently dropped.
# =============================================================================

# --- INPUT (Bronze layer) ---
BRONZE_YAHOO_DIR  = "data/bronze/yahoo_finance"
BRONZE_FRED_DIR   = "data/bronze/fred_macro"

# --- OUTPUT (Silver layer) ---
SILVER_YAHOO_DIR      = "data/silver/yahoo_finance"
SILVER_FRED_DIR       = "data/silver/fred_macro"
QUARANTINE_DIR        = "data/quarantine"

# Today's date for output filenames
TODAY = datetime.today().strftime("%Y-%m-%d")

# =============================================================================
# SECTION 4: HELPER — FIND LATEST BRONZE FILE
# Since Bronze filenames are date-stamped (e.g. yahoo_prices_2026-03-13.csv),
# this function finds the most recently created file in a folder automatically.
# That way we don't need to hardcode filenames — it always picks up the latest.
# =============================================================================

def get_latest_file(directory: str, prefix: str) -> str:
    """
    Finds the most recent CSV file in a directory matching a prefix.
    Example: get_latest_file('data/bronze/yahoo_finance', 'yahoo_prices')
    Returns the full file path of the latest matching file.
    """
    files = [
        f for f in os.listdir(directory)
        if f.startswith(prefix) and f.endswith(".csv")
    ]

    if not files:
        raise FileNotFoundError(f"No files found in {directory} with prefix '{prefix}'")

    # Sort alphabetically — date-stamped filenames sort chronologically
    latest = sorted(files)[-1]
    filepath = os.path.join(directory, latest)
    logger.info(f"Latest Bronze file found: {filepath}")
    return filepath

# =============================================================================
# SECTION 5: CREATE OUTPUT FOLDERS
# Creates Silver and Quarantine folders if they don't exist yet.
# =============================================================================

def create_output_dirs():
    """Creates all Silver and Quarantine output directories."""
    for path in [SILVER_YAHOO_DIR, SILVER_FRED_DIR, QUARANTINE_DIR]:
        os.makedirs(path, exist_ok=True)
    logger.info("Output directories ready.")

# =============================================================================
# SECTION 6: YAHOO FINANCE — SILVER TRANSFORMATION
# This is the core cleaning logic for stock price data. Each step is a
# specific data quality problem we saw in the Bronze file:
#
# Problem 1: Date has timezone attached (2024-03-13 00:00:00-04:00)
#            → Strip to just 2024-03-13
# Problem 2: Columns might have wrong data types
#            → Enforce correct types explicitly
# Problem 3: Duplicate rows might exist (same ticker, same date, twice)
#            → Deduplicate on ticker + date
# Problem 4: Invalid rows (null prices, zero/negative prices)
#            → Route to quarantine table, don't silently drop
# =============================================================================

def transform_yahoo(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and validates Yahoo Finance Bronze data.
    Returns a tuple of (clean_df, quarantine_df).
    """
    logger.info(f"Starting Yahoo Finance Silver transformation. Input rows: {len(df)}")
    quarantine_rows = []

    # --- STEP 1: Fix date column ---
    # The Bronze date looks like: 2024-03-13 00:00:00-04:00
    # We want just:               2024-03-13
    # .dt.tz_localize(None) strips the timezone, .dt.date gets just the date
    df["date"] = pd.to_datetime(df["date"], utc=True).dt.tz_localize(None).dt.normalize()
    df["date"] = df["date"].dt.date  # Convert to plain date (no time component)
    logger.info("Step 1 complete: Date timezone stripped.")

    # --- STEP 2: Enforce data types ---
    # Explicitly cast each column to its correct type.
    # This prevents downstream issues where a price might be stored as a string.
    df["open"]         = pd.to_numeric(df["open"],   errors="coerce")
    df["high"]         = pd.to_numeric(df["high"],   errors="coerce")
    df["low"]          = pd.to_numeric(df["low"],    errors="coerce")
    df["close"]        = pd.to_numeric(df["close"],  errors="coerce")
    df["volume"]       = pd.to_numeric(df["volume"], errors="coerce").astype("Int64")
    df["ticker"]       = df["ticker"].astype(str).str.upper().str.strip()
    logger.info("Step 2 complete: Data types enforced.")

    # --- STEP 3: Flag invalid rows for quarantine ---
    # Instead of silently dropping bad rows, we tag them with a reason
    # and route them to a separate quarantine file.
    # This is important for data lineage — you always want to know what was rejected and why.

    def flag_invalid(row):
        """Returns a rejection reason string if the row is invalid, else None."""
        if pd.isnull(row["close"]) or pd.isnull(row["open"]):
            return "null_price"
        if row["close"] <= 0 or row["open"] <= 0:
            return "non_positive_price"
        if row["high"] < row["low"]:
            return "high_less_than_low"
        if pd.isnull(row["ticker"]) or row["ticker"] == "":
            return "missing_ticker"
        return None

    df["rejection_reason"] = df.apply(flag_invalid, axis=1)

    # Split into clean and quarantine
    quarantine_df = df[df["rejection_reason"].notna()].copy()
    clean_df      = df[df["rejection_reason"].isna()].copy()

    # Remove the rejection_reason column from clean data
    clean_df = clean_df.drop(columns=["rejection_reason"])

    if len(quarantine_df) > 0:
        logger.warning(f"Step 3: {len(quarantine_df)} rows quarantined.")
        logger.warning(f"Quarantine reasons:\n{quarantine_df['rejection_reason'].value_counts()}")
    else:
        logger.info("Step 3 complete: No invalid rows found.")

    # --- STEP 4: Deduplicate ---
    # A row is a duplicate if it has the same ticker AND date.
    # Keep the last occurrence (in case of re-ingestion with updated data).
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["ticker", "date"], keep="last")
    dupes_removed = before_dedup - len(clean_df)
    if dupes_removed > 0:
        logger.warning(f"Step 4: {dupes_removed} duplicate rows removed.")
    else:
        logger.info("Step 4 complete: No duplicates found.")

    # --- STEP 5: Add Silver metadata ---
    clean_df["silver_processed_date"] = TODAY
    clean_df["silver_version"]        = "1.0"

    logger.info(f"Yahoo Finance Silver transformation complete.")
    logger.info(f"Clean rows: {len(clean_df)} | Quarantined: {len(quarantine_df)}")

    return clean_df, quarantine_df

# =============================================================================
# SECTION 7: FRED MACRO — SILVER TRANSFORMATION
# Simpler than Yahoo Finance because FRED data is already fairly clean.
# Main tasks:
# - Enforce date type
# - Enforce numeric value type
# - Validate value ranges per indicator (CPI should never be negative, etc.)
# - Deduplicate on series_id + date
# =============================================================================

def transform_fred(df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans and validates FRED macro Bronze data.
    Returns a tuple of (clean_df, quarantine_df).
    """
    logger.info(f"Starting FRED Silver transformation. Input rows: {len(df)}")

    # --- STEP 1: Fix date column ---
    df["date"] = pd.to_datetime(df["date"]).dt.date
    logger.info("Step 1 complete: Date formatted.")

    # --- STEP 2: Enforce data types ---
    df["value"]       = pd.to_numeric(df["value"], errors="coerce")
    df["series_id"]   = df["series_id"].astype(str).str.strip()
    df["series_name"] = df["series_name"].astype(str).str.strip()
    logger.info("Step 2 complete: Data types enforced.")

    # --- STEP 3: Flag invalid rows ---
    # Define reasonable value ranges per indicator.
    # These are sanity checks — if CPI is -500 something went wrong.
    valid_ranges = {
        "FEDFUNDS": (0, 25),      # Fed Funds Rate: 0% to 25%
        "CPIAUCSL": (100, 500),   # CPI index: reasonable historical range
        "GS10":     (0, 20),      # 10Y Treasury: 0% to 20%
        "UNRATE":   (0, 30),      # Unemployment: 0% to 30%
        "GDP":      (1000, 100000), # GDP in billions: $1T to $100T
    }

    def flag_invalid(row):
        if pd.isnull(row["value"]):
            return "null_value"
        series = row["series_id"]
        if series in valid_ranges:
            min_val, max_val = valid_ranges[series]
            if not (min_val <= row["value"] <= max_val):
                return f"out_of_range_{series}"
        return None

    df["rejection_reason"] = df.apply(flag_invalid, axis=1)

    quarantine_df = df[df["rejection_reason"].notna()].copy()
    clean_df      = df[df["rejection_reason"].isna()].copy()
    clean_df      = clean_df.drop(columns=["rejection_reason"])

    if len(quarantine_df) > 0:
        logger.warning(f"Step 3: {len(quarantine_df)} rows quarantined.")
    else:
        logger.info("Step 3 complete: No invalid rows found.")

    # --- STEP 4: Deduplicate on series_id + date ---
    before_dedup = len(clean_df)
    clean_df = clean_df.drop_duplicates(subset=["series_id", "date"], keep="last")
    dupes_removed = before_dedup - len(clean_df)
    if dupes_removed > 0:
        logger.warning(f"Step 4: {dupes_removed} duplicate rows removed.")
    else:
        logger.info("Step 4 complete: No duplicates found.")

    # --- STEP 5: Add Silver metadata ---
    clean_df["silver_processed_date"] = TODAY
    clean_df["silver_version"]        = "1.0"

    logger.info(f"FRED Silver transformation complete.")
    logger.info(f"Clean rows: {len(clean_df)} | Quarantined: {len(quarantine_df)}")

    return clean_df, quarantine_df

# =============================================================================
# SECTION 8: SAVE TO SILVER LAYER
# Saves clean data to Silver folder and quarantined rows to Quarantine folder.
# Both are date-stamped CSVs — same append-only principle as Bronze.
# =============================================================================

def save_to_silver(clean_df: pd.DataFrame, quarantine_df: pd.DataFrame,
                   silver_dir: str, quarantine_dir: str, prefix: str) -> None:
    """Saves clean and quarantine DataFrames to their respective folders."""

    # Save clean data
    clean_path = os.path.join(silver_dir, f"{prefix}_silver_{TODAY}.csv")
    clean_df.to_csv(clean_path, index=False)
    logger.info(f"Clean data saved: {clean_path} ({len(clean_df)} rows)")

    # Save quarantine data (even if empty — good for auditing)
    quarantine_path = os.path.join(quarantine_dir, f"{prefix}_quarantine_{TODAY}.csv")
    quarantine_df.to_csv(quarantine_path, index=False)
    logger.info(f"Quarantine data saved: {quarantine_path} ({len(quarantine_df)} rows)")


# =============================================================================
# SECTION 9: MAIN FUNCTION
# Orchestrates the full Bronze → Silver transformation for both sources.
# Pattern: load Bronze → transform → validate → save Silver + Quarantine
# =============================================================================

def main():
    logger.info("=" * 60)
    logger.info("Starting Bronze → Silver transformation")
    logger.info(f"Date: {TODAY}")
    logger.info("=" * 60)

    # Create output folders
    create_output_dirs()

    # -------------------------------------------------------------------------
    # YAHOO FINANCE
    # -------------------------------------------------------------------------
    logger.info("--- Processing Yahoo Finance ---")
    yahoo_bronze_path = get_latest_file(BRONZE_YAHOO_DIR, "yahoo_prices")
    df_yahoo_bronze   = pd.read_csv(yahoo_bronze_path)
    logger.info(f"Loaded {len(df_yahoo_bronze)} rows from Bronze.")

    df_yahoo_clean, df_yahoo_quarantine = transform_yahoo(df_yahoo_bronze)
    save_to_silver(df_yahoo_clean, df_yahoo_quarantine,
                   SILVER_YAHOO_DIR, QUARANTINE_DIR, "yahoo_finance")

    # -------------------------------------------------------------------------
    # FRED MACRO
    # -------------------------------------------------------------------------
    logger.info("--- Processing FRED Macro ---")
    fred_bronze_path = get_latest_file(BRONZE_FRED_DIR, "fred_macro")
    df_fred_bronze   = pd.read_csv(fred_bronze_path)
    logger.info(f"Loaded {len(df_fred_bronze)} rows from Bronze.")

    df_fred_clean, df_fred_quarantine = transform_fred(df_fred_bronze)
    save_to_silver(df_fred_clean, df_fred_quarantine,
                   SILVER_FRED_DIR, QUARANTINE_DIR, "fred_macro")

    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    logger.info("=" * 60)
    logger.info("Bronze → Silver transformation complete!")
    logger.info(f"Yahoo Finance: {len(df_yahoo_clean)} clean | {len(df_yahoo_quarantine)} quarantined")
    logger.info(f"FRED Macro:    {len(df_fred_clean)} clean | {len(df_fred_quarantine)} quarantined")
    logger.info("=" * 60)


if __name__ == "__main__":
    main()
