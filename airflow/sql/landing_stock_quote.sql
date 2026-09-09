-- Phase 2 §13 — target objects for the `stock_market_landing` DAG.
--
-- The DAG issues both statements itself on every run (they are IF NOT EXISTS and
-- therefore free), so this file is not what creates them in normal operation. It
-- exists for two other reasons:
--
--   1. It is the readable record of the schema. A reader should not have to open
--      a Python file to learn the shape of a table.
--   2. It is what you run as the `default` superuser on the ClickHouse LXC when
--      bootstrapping a fresh server — including the GRANT at the bottom, which
--      the DAG can NEVER issue for itself.
--
-- Run as the superuser, on the LXC:
--     ssh -i ~/.ssh/id_proxmox root@<clickhouse-host>
--     clickhouse-client --multiquery < landing_stock_quote.sql
--
-- The real address is deliberately not written here. This repository is public
-- and Phase 1 §4.8 removed every private address from it; the host lives in
-- `homelab-infra` and in the credentials inventory.

CREATE DATABASE IF NOT EXISTS landing;

CREATE TABLE IF NOT EXISTS landing.stock_quote
(
    ticker          String,
    current_price   Float64,
    change          Float64,
    change_pct      Float64,
    high            Float64,
    low             Float64,
    open            Float64,
    previous_close  Float64,
    api_timestamp   DateTime,
    ingested_at     DateTime DEFAULT now()
)
ENGINE = MergeTree
ORDER BY (ticker, ingested_at);

-- ⚠️ THIS GRANT IS NOT OPTIONAL AND IS EASY TO MISS.
--
-- The application account `elt_writer` was created with rights on database `elt`
-- and on nothing else. Without the line below, the DAG's very first statement
-- fails with:
--
--     Code: 497. DB::Exception: elt_writer: Not enough privileges. To execute
--     this query, it's necessary to have the grant CREATE DATABASE ON landing.*
--
-- which reads like a connection problem and is not one.
--
-- `elt_writer` cannot run this statement. Only the `default` superuser can, and
-- `default` is confined to localhost on the LXC, so it has to be run over SSH.
GRANT SELECT, INSERT, ALTER, CREATE DATABASE, CREATE TABLE, DROP TABLE, TRUNCATE
    ON landing.* TO elt_writer;

-- Verification query from the requirement:
--     SELECT * FROM landing.stock_quote ORDER BY ingested_at DESC;
