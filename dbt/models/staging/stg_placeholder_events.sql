-- PLACEHOLDER MODEL — Phase 1 §6.1.
--
-- It exists so the CI pipeline has something real to run: `dbt parse`, `dbt compile`
-- and `dbt test` all need a resolvable model, and a workflow that compiles nothing
-- proves nothing. Phase 2 step 8 replaces this with the real staging layer; the CI
-- workflow does not change when that happens.
--
-- Written with literal rows and no `source()` on purpose: it must build identically
-- on DuckDB (CI) and ClickHouse (in-cluster) with no data loaded and no seed step.
-- Nothing here is adapter-specific SQL.

select 1 as event_id, 'signup'   as event_type, 'alice' as actor
union all
select 2 as event_id, 'login'    as event_type, 'bob'   as actor
union all
select 3 as event_id, 'purchase' as event_type, 'alice' as actor
