# Airflow image and DAGs

This directory builds `ghcr.io/rizkiafdl/airflow`, the image the homelab's Airflow
runs. `Dockerfile` installs `requirements.txt` against the Airflow constraints
file and copies `dags/` to `/opt/airflow/dags`.

Two DAGs live here, and they share nothing on purpose:

| DAG | What it is |
|---|---|
| `elt_project` | The dbt pipeline, rendered by Cosmos from a synced `manifest.json`. No task in it has run yet — execution is Phase 2 §10. |
| `stock_market_landing` | A standalone hourly ingestion: Finnhub API → ClickHouse. Two plain tasks, no dbt, no Cosmos, no task pods. |

---

## `stock_market_landing`

Fetches the `AAPL` quote from Finnhub once an hour and appends it to
`landing.stock_quote` in ClickHouse.

```
Finnhub /quote  →  fetch_aapl_quote  →  insert_aapl_quote  →  landing.stock_quote
```

* **Schedule** `0 * * * *`, `Asia/Jakarta`
* **Retries** 2, five minutes apart
* **`catchup=False`** — required for correctness, not convenience. Finnhub's
  `/quote` returns the *current* quote and takes no date parameter, so a
  backfilled run would write today's price under an older run's timestamp.
* **`max_active_runs=1`** for the same reason.

### Configuration

Nothing is hardcoded. The DAG reads three variables from its environment:

| Variable | Required | Where it comes from |
|---|---|---|
| `FINNHUB_API_KEY` | **yes** | Kubernetes Secret `elt/finnhub-api`, key `api-key` |
| `CLICKHOUSE_PASSWORD` | **yes** | Kubernetes Secret `elt/clickhouse-elt-writer`, key `password` |
| `CLICKHOUSE_HOST` | **yes** | plain value in the HelmRelease — private, so not in this repository |
| `CLICKHOUSE_PORT` | no, defaults to `8123` | HelmRelease |
| `CLICKHOUSE_USER` | no, defaults to `elt_writer` | HelmRelease |

All three are injected by the Airflow chart's top-level `secret:` and `env:`
lists, which live in the **private** `homelab-infra` repository at
`flux/airflow/helmrelease.yaml`. Neither Secret is stored in Git in either
repository.

> **This repository is public.** No API key, password or private address belongs
> in any file here. Phase 1 §4.8 swept the tree once; keep it swept.

Both secret values are passed to Airflow's `mask_secret` when the module is
imported, so a traceback that captures one prints `***`.

To create the Secrets by hand:

```sh
kubectl create secret generic finnhub-api -n elt \
  --from-literal=api-key='<key>'

kubectl create secret generic clickhouse-elt-writer -n elt \
  --from-literal=password='<password>'
```

### ClickHouse target

`sql/landing_stock_quote.sql` holds the DDL. The DAG runs the `CREATE DATABASE`
and `CREATE TABLE` statements itself on every run — both are `IF NOT EXISTS` — so
the file is documentation plus a bootstrap script.

**The `GRANT` at the bottom of that file cannot be issued by the DAG.** The
application account is scoped to one database and has no rights on `landing`
until a superuser grants them. Skipping it produces:

```
Code: 497. DB::Exception: <user>: Not enough privileges. To execute this query,
it's necessary to have the grant CREATE DATABASE ON landing.*
```

which reads like a bad password and is not one.

### Duplicate handling

The requirement asks for two things that disagree — *preserve every hourly
observation* and *do not duplicate on retry, keyed on `ticker + api_timestamp`*.
They conflict whenever the market is closed, because Finnhub then returns the
same `t` hour after hour, and a global uniqueness guard would drop every
overnight row.

So the guard is **scoped to the DAG run**. Before inserting, the task checks
whether a row with this `(ticker, api_timestamp)` was ingested at or after the
run's `data_interval_start`:

* a retry inside the same hour finds its own earlier row and skips;
* the next hourly run has a later window and inserts, even when `t` has not moved.

The table stays `MergeTree ORDER BY (ticker, ingested_at)` exactly as specified.
It is deliberately **not** `ReplacingMergeTree`, which would dedupe on
`ingested_at` — collapsing nothing useful, and only at merge time.

### Timezones

The **schedule** is `Asia/Jakarta`. The **stored** timestamps are UTC, because
the ClickHouse server runs `Etc/UTC` and a `DateTime` column carries no zone.
Finnhub's `t` is a Unix epoch, so the conversion is exact. The offset you see
between the Airflow UI and a `SELECT` is correct — do not "fix" it.

### What makes the DAG fail

By design, all of these are red tasks and never a silent empty row:

* any non-2xx from Finnhub (`raise_for_status`);
* a connect or read timeout (`(5, 15)` seconds);
* a response that is not a JSON object;
* any of `c d dp h l o pc t` missing — the error names all of them at once;
* **an all-zero quote.** Finnhub answers `HTTP 200` with zeros for an unknown
  symbol, a revoked token, or an endpoint outside the plan. There is no error
  code. Checking the status alone would land a row that looks like a crash.
* a missing `FINNHUB_API_KEY`, `CLICKHOUSE_PASSWORD` or `CLICKHOUSE_HOST` — the
  error names the variable, so a deployment fault is not mistaken for a data one;
* any ClickHouse failure.

### Running and testing

The DAG deploys the same way as everything else: push to `main`, CI builds the
image, and Flux image automation rolls it out. No manual step.

Trigger a run by hand and watch it:

```sh
kubectl exec -n elt airflow-scheduler-0 -c scheduler -- \
  airflow dags trigger stock_market_landing

kubectl exec -n elt airflow-scheduler-0 -c scheduler -- \
  airflow tasks states-for-dag-run stock_market_landing <run_id>
```

Check the result:

```sql
SELECT * FROM landing.stock_quote ORDER BY ingested_at DESC;
```

Or through the UI:

```sh
kubectl port-forward -n elt svc/airflow-api-server 8080:8080
```

> On Airflow 3 the UI component is `api-server`. There is no `webserver` Service.
