-- =============================================================================
-- MART MODEL: mrt_moving_averages
-- Calculates 7-day, 30-day, and 90-day moving averages per ticker.
-- Moving averages smooth out price volatility and are one of the most
-- commonly used indicators in financial analysis.
-- =============================================================================

with daily_returns as (

    select * from "market_intelligence"."main"."mrt_daily_returns"

),

moving_averages as (

    select
        ticker,
        price_date,
        close_price,
        daily_return_pct,

        -- 7-day moving average (short term trend)
        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 6 preceding and current row
        ), 4) as ma_7d,

        -- 30-day moving average (medium term trend)
        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ), 4) as ma_30d,

        -- 90-day moving average (long term trend)
        round(avg(close_price) over (
            partition by ticker
            order by price_date
            rows between 89 preceding and current row
        ), 4) as ma_90d,

        -- 30-day rolling volatility (annualized standard deviation)
        round(stddev(daily_return_pct) over (
            partition by ticker
            order by price_date
            rows between 29 preceding and current row
        ) * sqrt(252), 4) as volatility_30d_annualized

    from daily_returns

)

select * from moving_averages