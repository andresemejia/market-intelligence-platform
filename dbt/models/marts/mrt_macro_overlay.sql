-- =============================================================================
-- MART MODEL: mrt_macro_overlay
-- Joins macro indicators from FRED to daily stock prices.
-- This lets us answer questions like:
-- "How did tech stocks perform when the Fed raised rates?"
-- "Is there a correlation between CPI and energy stocks?"
--
-- Since FRED data is monthly and stock data is daily, we use a
-- date spine approach — each stock date gets the most recent
-- macro reading available (forward fill).
-- =============================================================================

with prices as (

    select * from {{ ref('mrt_daily_returns') }}

),

macro as (

    select * from {{ ref('stg_fred_macro') }}

),

-- Pivot macro data from long format to wide format
-- Long:  one row per indicator per date
-- Wide:  one row per date with each indicator as its own column
macro_pivoted as (

    select
        macro_date,

        max(case when series_id = 'FEDFUNDS' then indicator_value end) as fed_funds_rate,
        max(case when series_id = 'CPIAUCSL' then indicator_value end) as cpi,
        max(case when series_id = 'GS10'     then indicator_value end) as treasury_yield_10y,
        max(case when series_id = 'UNRATE'   then indicator_value end) as unemployment_rate,
        max(case when series_id = 'GDP'      then indicator_value end) as gdp

    from macro
    group by macro_date

),

-- Join stock prices to most recent macro reading
-- asof join matches each stock date to the nearest macro date
-- that is less than or equal to the stock date
joined as (

    select
        p.ticker,
        p.price_date,
        p.close_price,
        p.daily_return_pct,
        m.macro_date,
        m.fed_funds_rate,
        m.cpi,
        m.treasury_yield_10y,
        m.unemployment_rate,
        m.gdp

    from prices p
    asof join macro_pivoted m
        on p.price_date >= m.macro_date

)

select * from joined
order by ticker, price_date
