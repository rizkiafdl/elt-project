-- Phase 2 §8.6 — a REAL data test, not a schema test.
--
-- WHAT IT ASSERTS, AND WHY IT IS WORTH ASSERTING. Finnhub returns the last traded
-- price (`c`) and the session's own high and low (`h`, `l`) in the SAME payload.
-- A quote whose price sits outside its own session range is internally
-- inconsistent: either the feed disagreed with itself, or this pipeline mixed
-- fields from two different payloads. Neither is visible to a `not_null` test, and
-- neither would stop a single model from building.
--
-- ⚠️ THIS TEST CAN GO RED WITHOUT ANY CODE CHANGING, because it tests the DATA and
-- the data arrives from a third party. That is the point of a data test, and it is
-- also why it must be diagnosable: the columns below are chosen so the failing row
-- explains itself without a second query.
--
-- Measured before it was written: 0 violating rows out of 4 (2026-09-10).
--
-- A dbt test passes when it returns NO rows.

select
    ticker,
    quoted_at,
    current_price,
    high_price,
    low_price,
    ingested_at
from {{ ref('stg_stock_quote') }}
where current_price > high_price
   or current_price < low_price
