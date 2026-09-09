-- PLACEHOLDER MODEL — Phase 1 §6.1.
--
-- It exists so the CI pipeline has something real to resolve: `dbt parse` needs a
-- resolvable model, and a workflow that parses nothing proves nothing. Phase 2 step 8
-- replaces this with the real staging layer.
--
-- ⚠️ THE REASON IT HAS NO `source()` HAS CHANGED, AND THAT MATTERS FOR ITS
-- REPLACEMENT. It was written with literal rows so it would build identically on
-- DuckDB (CI) and ClickHouse (in-cluster) with no data loaded. The DuckDB target was
-- removed on 2026-09-10, so "builds on both" is no longer a requirement of anything.
-- What remains true is narrower: CI only PARSES, so a model here needs to resolve, not
-- to have data behind it. The real staging layer WILL read `source()` — and it will be
-- parsed in CI and executed only in the cluster.

select 1 as event_id, 'signup'   as event_type, 'alice' as actor
union all
select 2 as event_id, 'login'    as event_type, 'bob'   as actor
union all
select 3 as event_id, 'purchase' as event_type, 'alice' as actor
