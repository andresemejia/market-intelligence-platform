-- =============================================================================
-- MART MODEL: mrt_sector_performance
-- Aggregates daily returns by sector so we can compare how tech performed
-- vs finance vs healthcare etc. on any given day.
-- This powers the sector heatmap in the dashboard.
-- =============================================================================

with daily_returns as (

    select * from {{ ref('mrt_daily_returns') }}

),

-- Map each ticker to its sector
-- This is a static mapping — in a production system this would come
-- from a dimension table, but for our portfolio project we define it here
ticker_sectors as (

    select * from (values
        ('AAPL', 'Technology'),
        ('MSFT', 'Technology'),
        ('NVDA', 'Technology'),
        ('JPM',  'Financials'),
        ('GS',   'Financials'),
        ('BAC',  'Financials'),
        ('JNJ',  'Healthcare'),
        ('PFE',  'Healthcare'),
        ('UNH',  'Healthcare'),
        ('XOM',  'Energy'),
        ('CVX',  'Energy'),
        ('AMZN', 'Consumer'),
        ('WMT',  'Consumer'),
        ('COST', 'Consumer')
    ) as t(ticker, sector)

),

joined as (

    select
        dr.price_date,
        ts.sector,
        dr.ticker,
        dr.close_price,
        dr.daily_return_pct

    from daily_returns dr
    left join ticker_sectors ts
        on dr.ticker = ts.ticker

),

sector_aggregated as (

    select
        price_date,
        sector,

        -- Average return across all tickers in the sector that day
        round(avg(daily_return_pct), 4)     as avg_daily_return_pct,

        -- Best and worst performer in the sector that day
        round(max(daily_return_pct), 4)     as max_daily_return_pct,
        round(min(daily_return_pct), 4)     as min_daily_return_pct,

        -- Number of tickers in sector
        count(distinct ticker)              as ticker_count

    from joined
    group by price_date, sector

)

select * from sector_aggregated
order by price_date desc, sector
