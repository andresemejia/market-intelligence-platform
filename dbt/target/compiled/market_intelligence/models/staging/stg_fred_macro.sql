-- =============================================================================
-- STAGING MODEL: stg_fred_macro
-- Reads FRED macro Silver CSV into dbt.
-- =============================================================================

with source as (

    select * from read_csv_auto(
        '/Users/andresmejia/Documents/market-intelligence-platform/data/silver/fred_macro/fred_macro_silver_*.csv'
    )

),

renamed as (

    select
        cast(date       as date)        as macro_date,
        series_id,
        series_name,
        cast(value      as double)      as indicator_value,
        ingestion_date,
        silver_processed_date,
        source

    from source

)

select * from renamed