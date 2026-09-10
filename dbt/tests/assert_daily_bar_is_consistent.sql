-- Phase 2 §8.6 — a REAL data test on the mart, one layer further out.
--
-- The daily bar mixes two kinds of number by design (§8.5): `high_price` and
-- `low_price` are Finnhub's whole-session figures, while `close_price` is the last
-- price this estate OBSERVED by polling hourly. That design is only safe while the
-- observed price stays inside the session range it claims to belong to.
--
-- This test is the one that would notice if those two kinds of number were ever
-- folded together wrongly -- for example if a future edit took `close_price` from
-- a different day's rows, or if the daily fold started spanning two sessions.
--
-- It also catches the degenerate bar: a high below its own low.
--
-- Measured before it was written: 0 violating rows (2026-09-10).
--
-- A dbt test passes when it returns NO rows.

select
    ticker,
    quote_date,
    open_price,
    high_price,
    low_price,
    close_price,
    observation_count
from {{ ref('mart_stock_daily_ohlc') }}
where high_price < low_price
   or close_price > high_price
   or close_price < low_price
