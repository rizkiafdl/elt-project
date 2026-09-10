{{ config(
    materialized='table',
    engine='MergeTree()',
    order_by=['ticker', 'quote_date']
) }}

-- Phase 2 §11.6, condition 5 — the model that proves the pipeline is self-serving.
--
-- 🚩 WHY THIS MODEL EXISTS. §11.6's fifth gate condition is "a model added in
-- `elt-project` reaches production with NO MANUAL STEP". This file is that model.
-- It was pushed and then left alone: push -> CI parse -> dbt-runner + airflow
-- images -> manifest.json Release asset -> the `manifest-sync` CronJob at :17 ->
-- Cosmos renders two new tasks -> the next scheduled DAG run materialises the
-- table in ClickHouse. Nobody ran anything. That is the whole project in one
-- sentence, and it is the real exit gate of Phase 2.
--
-- It is a REAL model, not a placeholder. Deleting it removes a fact nothing else
-- carries; if it is ever dropped, drop the comment above with it and record why.
--
-- WHAT IT MEASURES: how much of a session's true range the estate's hourly
-- sampling actually saw.
--
-- ⚠️ THE TWO RANGES ARE DIFFERENT MEASUREMENTS AND MUST NOT BE COMPARED CASUALLY.
-- `session_range` comes from Finnhub's own `high`/`low`, which describe the WHOLE
-- session. `observed_range` comes from `current_price` values the loader happened
-- to poll, at most 24 of them. `observed_share_of_session_pct` is the ratio, and
-- it is the honest measure of the sampling gap this pipeline has by design.
--
-- 🚩 `nullIf` ON EVERY DENOMINATOR, ON PURPOSE. ClickHouse returns `inf` for
-- Float64 division by zero rather than raising, so a flat session (high == low,
-- true on a closed-market day) would otherwise write `inf` into a mart and look
-- like a real number. `nullIf(x, 0)` turns the denominator NULL and the result
-- NULL, which reads as "not measurable" instead of "infinite".
--
-- ⚠️ `quote_date` IS A UTC CALENDAR DAY, NOT A MARKET DAY -- the same caveat
-- every model carrying `quote_date` repeats. See `int_stock_quote_daily`.

select
    ticker,
    quote_date,

    -- Finnhub's session figures: the whole session
    session_high_price - session_low_price                      as session_range,
    round(
        100 * (session_high_price - session_low_price)
            / nullIf(session_open_price, 0), 4
    )                                                           as session_range_pct,

    -- what the estate actually sampled
    max_observed_price - min_observed_price                     as observed_range,
    round(
        100 * (max_observed_price - min_observed_price)
            / nullIf(session_high_price - session_low_price, 0), 4
    )                                                           as observed_share_of_session_pct,

    observation_count,
    distinct_quote_count,
    last_quoted_at

from {{ ref('int_stock_quote_daily') }}
