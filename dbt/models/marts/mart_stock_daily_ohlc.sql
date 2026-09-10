{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['ticker', 'quote_date']
) }}

-- Phase 2 §8.5 — a mart: one row per (ticker, quote_date), daily OHLC.
--
-- 🚩 THIS IS THE FIRST MODEL IN THE ESTATE CARRYING ClickHouse-SPECIFIC CONFIG.
-- `engine` and `order_by` are `dbt-clickhouse` configs and mean nothing to any
-- other adapter. §8.3 decided they are written plainly, in the model, because
-- there is only one adapter left to be portable to. ⚠️ Nothing validates these
-- keys: dbt accepts a misspelling silently and ClickHouse then builds the table
-- with a default sort order, which is a wrong result rather than an error
-- (measured -- see findings topic 31). Read them twice.
--
-- WHY A TABLE AND NOT A VIEW. Marts are what a consumer queries. Materializing
-- means the consumer's query does not re-run the whole staging chain, and
-- `order_by (ticker, quote_date)` is the sort a consumer will actually filter on.
-- The cost is that the table is only as fresh as the last dbt run.
--
-- ⚠️ `close_price` IS THE LAST PRICE THE ESTATE OBSERVED, NOT THE OFFICIAL CLOSE.
-- The loader samples hourly; the true closing print is not something this pipeline
-- sees. `open_price`, `high_price` and `low_price` ARE Finnhub's own session
-- figures and do describe the whole session. The two kinds of number sit in one
-- row and are not interchangeable -- hence the explicit names.

select
    ticker,
    quote_date,

    session_open_price      as open_price,
    session_high_price      as high_price,
    session_low_price       as low_price,
    last_observed_price     as close_price,
    previous_close_price,

    observation_count,
    distinct_quote_count,
    last_quoted_at,
    last_ingested_at

from {{ ref('int_stock_quote_daily') }}
