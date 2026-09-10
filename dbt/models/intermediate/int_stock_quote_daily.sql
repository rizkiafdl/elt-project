-- Phase 2 §8.5 — the intermediate layer: fold hourly observations into a day.
--
-- GRAIN: one row per (ticker, quote_date). Upstream is one row per
-- (ticker, quoted_at); this is the only place that changes.
--
-- 🚩 WHAT "A DAY" MEANS HERE, AND WHY IT IS NOT THE MARKET'S DAY. `quote_date`
-- comes from `toDate(quoted_at)` in staging and this ClickHouse runs on `Etc/UTC`,
-- so a day is a UTC calendar day. The New York session straddles that boundary --
-- a quote taken at 21:30 New York time is already the next UTC day. Fixing that
-- needs a market calendar, which is business logic with a data dependency this
-- estate does not have. It is recorded on every model that carries `quote_date`
-- rather than hidden in one place.
--
-- ⚠️ OBSERVATIONS ARE A SAMPLE, NOT A TAPE. The loader polls once an hour, so
-- `observation_count` is at most 24 and everything derived from `current_price`
-- describes only the moments the estate happened to look. Finnhub's own `high`,
-- `low` and `open` fields describe the whole session and are carried separately
-- for exactly that reason -- they are not the same measurement and must not be
-- averaged together.
--
-- 🚩 A CLOSED-MARKET DAY LOOKS BUSY. Finnhub keeps returning the last quote after
-- the close, so this model will happily report 13 observations of one frozen
-- price. `distinct_quote_count` exists to make that visible rather than
-- surprising: when it is 1 and `observation_count` is 13, nothing moved.

with quotes as (

    select * from {{ ref('stg_stock_quote') }}

)

select
    ticker,
    quote_date,

    -- how much the estate actually saw
    count()                                     as observation_count,
    count(distinct quoted_at)                   as distinct_quote_count,
    min(quoted_at)                              as first_quoted_at,
    max(quoted_at)                              as last_quoted_at,

    -- observed prices -- sampled hourly, see the note above
    argMin(current_price, quoted_at)            as first_observed_price,
    argMax(current_price, quoted_at)            as last_observed_price,
    min(current_price)                          as min_observed_price,
    max(current_price)                          as max_observed_price,

    -- Finnhub's own session figures, carried through unaveraged
    argMax(open_price, quoted_at)               as session_open_price,
    max(high_price)                             as session_high_price,
    min(low_price)                              as session_low_price,
    argMax(previous_close_price, quoted_at)     as previous_close_price,

    -- ingestion clock, kept so a day can be traced back to when it was learned
    min(ingested_at)                            as first_ingested_at,
    max(ingested_at)                            as last_ingested_at

from quotes
group by
    ticker,
    quote_date
