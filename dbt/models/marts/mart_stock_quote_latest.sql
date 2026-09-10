{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['ticker']
) }}

-- Phase 2 §8.5 — a mart: one row per ticker, the newest quote seen.
--
-- THIS IS THE "IS IT WORKING?" MODEL. It answers the one question a person asks
-- of this pipeline without wanting a query language: what is the latest price we
-- have, when was it quoted, and when did we learn it.
--
-- ⚠️ IT IS NOT A FRESHNESS CHECK, AND MUST NOT BE READ AS ONE. `last_ingested_at`
-- here is only as current as the last dbt run -- this is a table, and dbt runs on
-- a schedule of its own. `dbt source freshness` (§8.2) reads the landing table
-- directly and is the thing that knows whether the LOADER is alive. A stale row
-- here can mean a stopped loader or an unrun transform, and it cannot tell you
-- which.
--
-- `argMax` is ClickHouse's idiom for "the value from the row with the largest
-- key". One pass, no window function, no self-join.
--
-- 🚩 WHY THE AGGREGATE IS ALIASED `latest_quoted_at` AND RENAMED AFTERWARDS,
-- RATHER THAN WRITTEN AS `max(quoted_at) AS quoted_at`. ClickHouse resolves a
-- select-list alias BEFORE it resolves a column of the same name, so the obvious
-- form makes every `argMax(..., quoted_at)` beside it read the alias -- an
-- aggregate inside an aggregate -- and the query is rejected:
--
--   Code: 184. DB::Exception: Aggregate function max(quoted_at) AS quoted_at is
--   found inside another aggregate function in query. (ILLEGAL_AGGREGATION)
--
-- ⚠️ `dbt parse` accepts that query without a warning. It was caught by running
-- the SQL against ClickHouse before pushing, which is the practice §8.3 exists to
-- justify: CI checks the graph, never the SQL.

with latest as (

    select
        ticker,
        max(quoted_at)                              as latest_quoted_at,
        argMax(quote_date, quoted_at)               as quote_date,
        argMax(current_price, quoted_at)            as current_price,
        argMax(price_change, quoted_at)             as price_change,
        argMax(price_change_pct, quoted_at)         as price_change_pct,
        argMax(previous_close_price, quoted_at)     as previous_close_price,
        argMax(ingested_at, quoted_at)              as ingested_at

    from {{ ref('stg_stock_quote') }}
    group by ticker

)

select
    ticker,
    latest_quoted_at as quoted_at,
    quote_date,
    current_price,
    price_change,
    price_change_pct,
    previous_close_price,
    ingested_at

from latest
