-- =============================================================================
-- STAGING MODEL: stg_yahoo_prices
-- Reads from the Silver layer CSV and presents it as a clean view in DuckDB.
-- Staging models are the entry point into dbt — they rename columns,
-- cast types, and do nothing else. No business logic here.
-- =============================================================================

with source as (

    select * from read_csv_auto(
        '/Users/andresmejia/Documents/market-intelligence-platform/data/silver/yahoo_finance/yahoo_finance_silver_*.csv'
    )

),

renamed as (

    select
        -- Identifiers
        ticker,

        -- Dates
        cast(date as date)          as price_date,

        -- Prices
        cast(open  as double)       as open_price,
        cast(high  as double)       as high_price,
        cast(low   as double)       as low_price,
        cast(close as double)       as close_price,

        -- Volume
        cast(volume as bigint)      as volume,

        -- Metadata
        ingestion_date,
        silver_processed_date,
        source

    from source

)

select * from renamed