
  
    
    

    create  table
      "market_intelligence"."main"."mrt_daily_returns__dbt_tmp"
  
    as (
      -- =============================================================================
-- MART MODEL: mrt_daily_returns
-- Calculates daily percentage return per ticker.
-- This is the foundation for all performance analysis in the dashboard.
-- Formula: (today's close - yesterday's close) / yesterday's close * 100
-- =============================================================================

with prices as (

    select * from "market_intelligence"."main"."stg_yahoo_prices"

),

with_returns as (

    select
        ticker,
        price_date,
        open_price,
        high_price,
        low_price,
        close_price,
        volume,

        -- Previous day's close using LAG window function
        lag(close_price) over (
            partition by ticker
            order by price_date
        ) as prev_close_price,

        -- Daily return %
        round(
            (close_price - lag(close_price) over (
                partition by ticker order by price_date
            )) / lag(close_price) over (
                partition by ticker order by price_date
            ) * 100,
        4) as daily_return_pct

    from prices

)

select * from with_returns
where prev_close_price is not null
    );
  
  