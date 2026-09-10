-- Phase 2 §8.4 — the first real staging model, and the first dbt model in this
-- estate that reads data somebody else wrote.
--
-- GRAIN: one row per (ticker, quoted_at). The landing table does NOT have that
-- grain, and that is the whole reason this model does more than rename columns.
--
-- ⚠️ WHY DE-DUPLICATION IS NOT OPTIONAL HERE, MEASURED RATHER THAN ASSUMED. The
-- §13 loader skips a quote it has already seen, but it compares only against the
-- CURRENT scheduling window: `WHERE ingested_at >= <window start>`. A retry in a
-- LATER window therefore re-inserts a quote it already has, because the guard no
-- longer sees the earlier row. §13's own gate produced three runs and two rows for
-- exactly this reason -- the third was skipped only because it fell inside the same
-- window. The natural key is unique per window and NOT unique across all time.
-- Staging is where it is made true.
--
-- KEEP THE EARLIEST `ingested_at`. When the same quote is landed twice, the first
-- landing is the one that reflects when this estate actually learned the value. The
-- later copy is a retry artefact and carries no new information.
--
-- 🚩 STAGING RULES FROM THE PLAN (Layer 3 §2), and where this model sits against
-- them: "one model per source table, renaming and casting only, no joins and no
-- business logic". There are no joins and no business logic. The de-duplication is
-- neither -- it is the model asserting its own declared grain, and it was written
-- into this layer's contract at §8.1 before any model existed.
--
-- ⚠️ `toDate()` USES THE SERVER TIMEZONE, which is `Etc/UTC` on this ClickHouse
-- (source: `SELECT timezone()`). `quote_date` is therefore a UTC calendar date, not
-- a US market date -- a quote taken at 21:30 New York time lands on the NEXT UTC
-- day. That is a deliberate, recorded choice: the estate has one clock, and a
-- market calendar is business logic that belongs in `marts/`, not here.

with ranked as (

    select
        ticker,
        api_timestamp,
        current_price,
        change,
        change_pct,
        high,
        low,
        open,
        previous_close,
        ingested_at,

        -- One row survives per (ticker, api_timestamp): the earliest landing.
        row_number() over (
            partition by ticker, api_timestamp
            order by ingested_at asc
        ) as landing_rank

    from {{ source('landing', 'stock_quote') }}

)

select
    -- keys
    ticker,
    api_timestamp                as quoted_at,
    toDate(api_timestamp)        as quote_date,

    -- measures, renamed so the unit is readable without the Finnhub docs open
    current_price,
    change                       as price_change,
    change_pct                   as price_change_pct,
    high                         as high_price,
    low                          as low_price,
    open                         as open_price,
    previous_close               as previous_close_price,

    -- ingestion clock, carried through unchanged; freshness is measured on it
    ingested_at

from ranked
where landing_rank = 1
