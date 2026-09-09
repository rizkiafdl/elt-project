"""Hourly Finnhub -> ClickHouse landing ingestion — Phase 2 §13.

WHAT THIS DAG IS FOR
--------------------
It is the estate's first END-TO-END test of the stack with a real external
source: pull a quote from a public HTTP API, land it in ClickHouse, keep every
hourly observation. It deliberately shares nothing with ``elt_dag.py`` — no
Cosmos, no dbt, no manifest, no task pods. Two plain tasks in the scheduler
process. If this DAG works and ``elt_dag`` does not, the fault is in the dbt
half, not in Airflow or in the network path to ClickHouse.

WHERE THE SECRETS COME FROM, AND WHY NOT FROM THIS FILE
-------------------------------------------------------
``elt-project`` is a PUBLIC repository and this file is baked into the Airflow
image, so anything written here is published twice over. Both credentials arrive
as environment variables, injected from Kubernetes Secrets by the chart's
top-level ``secret:`` list (see ``homelab-infra/flux/airflow/helmrelease.yaml``):

    FINNHUB_API_KEY        <- Secret elt/finnhub-api           key: api-key
    CLICKHOUSE_PASSWORD    <- Secret elt/clickhouse-elt-writer  key: password
    CLICKHOUSE_HOST        <- plain `env:` value in the HelmRelease (not secret,
                              but private — this repository is public, see below)

Neither Secret is in Git. They are created out of band and recorded in
``artifacts/inventory/credentials.md``. Both values are passed to
``mask_secret`` at import time, so a stack trace that happens to contain one
prints ``***`` instead.

TIMEZONES — TWO OF THEM, ON PURPOSE
-----------------------------------
The SCHEDULE is Asia/Jakarta, because that is where the person reading the UI
is. The STORED timestamps are UTC, because the ClickHouse server runs
``Etc/UTC`` (verified: ``SELECT timezone()`` -> ``Etc/UTC``) and a ``DateTime``
column carries no zone of its own. Finnhub's ``t`` is a Unix epoch, which is
zone-free, so the conversion is exact. Do not "fix" the shift you will see
between the UI and a ``SELECT`` — a Jakarta 09:00 run correctly lands 02:00.

DUPLICATE HANDLING — THE SPEC'S TWO RULES CONFLICT, AND THIS IS THE RESOLUTION
------------------------------------------------------------------------------
The requirement asks for both "preserve every hourly ingestion" and "on retry,
avoid duplicates keyed on ticker + api_timestamp". Those disagree whenever the
market is closed: Finnhub then returns the SAME ``t`` hour after hour, so a
global uniqueness guard on ``(ticker, api_timestamp)`` would silently drop every
overnight observation — the opposite of what a landing layer is for.

The guard is therefore scoped to the CURRENT DAG RUN. Before inserting, the task
asks whether a row with this ``(ticker, api_timestamp)`` was already ingested at
or after this run's ``data_interval_start``. A retry inside the same hour finds
its own earlier row and skips; next hour's run has a later window and inserts,
even when ``t`` has not moved.

    retry within the hour   -> row exists in window -> SKIP
    next hourly run         -> window moved         -> INSERT

The table is left exactly as the requirement specifies — ``MergeTree ORDER BY
(ticker, ingested_at)``. It is NOT ``ReplacingMergeTree``: that would dedupe on
``ORDER BY``, which here is ``ingested_at``, so it would collapse nothing that
matters and would make the landing layer lossy in a way that is invisible until
a merge runs.
"""

from __future__ import annotations

import os
from datetime import timedelta

import pendulum
import requests
from airflow.sdk import dag, task
from airflow.sdk._shared.secrets_masker import mask_secret

# ── CONFIGURATION ──────────────────────────────────────────────────────────────
TICKER = "AAPL"
FINNHUB_QUOTE_URL = "https://finnhub.io/api/v1/quote"

# Both halves of the timeout matter. The first is how long to wait for the TCP
# connect, the second for the body. A single scalar would let a half-open socket
# hold the task open for the whole value.
HTTP_TIMEOUT = (5, 15)

# ⚠️ NO DEFAULT HOST, ON PURPOSE — THIS REPOSITORY IS PUBLIC.
# Phase 1 §4.8 removed every private address from the published tree and the rule
# stands: no address of this estate is ever committed here. The host arrives as an
# environment variable from `homelab-infra`, which is private. A hardcoded
# fallback here would put the estate's topology back into a public repository for
# the sake of saving one line of Helm values.
#
# It is read inside the task, not at module scope, so a missing value produces a
# RED TASK with a named variable rather than an import error that makes the whole
# DAG disappear from the UI.
CLICKHOUSE_DATABASE = "landing"
CLICKHOUSE_TABLE = "stock_quote"

# ⚠️ elt_writer's DEFAULT database is `default`, which it has no grant on. Every
# connection must name the database explicitly or the first query fails with an
# access error that reads like a wrong password.
#
# The grant on `landing` was added by hand as the `default` superuser on the LXC;
# it did not come with the account. See §13 in the working items.

# The eight fields the API contract promises. Checked as a set, not one by one,
# so a partial response names everything that is missing in one message.
REQUIRED_FIELDS = ("c", "d", "dp", "h", "l", "o", "pc", "t")

# Finnhub -> column. Kept as data rather than as eight assignments so the mapping
# can be read against the requirement's table without tracing code.
FIELD_TO_COLUMN = {
    "c": "current_price",
    "d": "change",
    "dp": "change_pct",
    "h": "high",
    "l": "low",
    "o": "open",
    "pc": "previous_close",
}

# Masked at import time, in the dag-processor AND in the scheduler, so the value
# is already registered before any task can raise with it in scope.
for _var in ("FINNHUB_API_KEY", "CLICKHOUSE_PASSWORD"):
    _value = os.environ.get(_var)
    if _value:
        mask_secret(_value)


def _require_env(name: str) -> str:
    """Read a required secret from the environment, or fail loudly.

    A missing Secret is a deployment error, not a data error. Failing here — with
    the variable named — is what keeps it from surfacing later as a 401 from
    Finnhub or as an authentication failure from ClickHouse, either of which
    would send the reader looking at the wrong system.
    """
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(
            f"{name} is not set. It is injected from a Kubernetes Secret by the "
            f"chart's top-level `secret:` list in homelab-infra/flux/airflow/"
            f"helmrelease.yaml. Check that the Secret exists in namespace `elt`."
        )
    return value


@dag(
    dag_id="stock_market_landing",
    description="Hourly AAPL quote from Finnhub, landed in ClickHouse landing.stock_quote",
    # Every hour, on the hour, Jakarta time.
    schedule="0 * * * *",
    start_date=pendulum.datetime(2026, 9, 10, tz="Asia/Jakarta"),
    # No backfill. Finnhub's /quote endpoint returns the CURRENT quote and takes
    # no date parameter, so a backfilled run cannot fetch the hour it claims to
    # represent — it would write today's price under yesterday's run and make the
    # landing table lie. catchup=False is a correctness requirement here, not a
    # convenience.
    catchup=False,
    # And for the same reason, never run two of these at once.
    max_active_runs=1,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=5),
    },
    tags=["landing", "clickhouse", "finnhub", "stock"],
    doc_md=__doc__,
)
def stock_market_landing():
    @task(task_id="fetch_aapl_quote")
    def fetch_aapl_quote() -> dict:
        """Call Finnhub, validate hard, and return a row-shaped dict."""
        api_key = _require_env("FINNHUB_API_KEY")

        response = requests.get(
            FINNHUB_QUOTE_URL,
            params={"symbol": TICKER, "token": api_key},
            timeout=HTTP_TIMEOUT,
        )
        # raise_for_status covers every non-2xx, which is what the requirement
        # asks for. requests raises Timeout and ConnectionError on its own; both
        # are left to propagate so Airflow's retries can act on them.
        response.raise_for_status()

        payload = response.json()
        if not isinstance(payload, dict):
            raise ValueError(f"Finnhub returned {type(payload).__name__}, expected an object")

        missing = [field for field in REQUIRED_FIELDS if payload.get(field) is None]
        if missing:
            raise ValueError(
                f"Finnhub response is missing required field(s) {missing}. "
                f"Received keys: {sorted(payload)}"
            )

        # 🚩 FINNHUB ANSWERS HTTP 200 WITH ALL-ZERO FIELDS FOR AN UNKNOWN SYMBOL
        # OR AN OUT-OF-PLAN ENDPOINT. There is no error code and no message. A
        # zero current price and a zero timestamp are therefore the real failure
        # signal, and checking the status code alone would land a row of zeros
        # that looks like a market crash.
        if float(payload["c"]) == 0.0 or int(payload["t"]) == 0:
            raise ValueError(
                f"Finnhub returned an empty quote for {TICKER} "
                f"(c={payload['c']}, t={payload['t']}). This is what an unknown symbol, "
                f"a revoked token or a plan restriction looks like — HTTP 200 with zeros."
            )

        # Unix epoch -> naive UTC, which is what the DateTime column stores.
        api_timestamp = pendulum.from_timestamp(int(payload["t"]), tz="UTC")

        row = {"ticker": TICKER, "api_timestamp": api_timestamp.to_datetime_string()}
        row.update({column: float(payload[field]) for field, column in FIELD_TO_COLUMN.items()})
        return row

    @task(task_id="insert_aapl_quote")
    def insert_aapl_quote(row: dict, **context) -> None:
        """Ensure the target exists, guard against a retry duplicate, insert."""
        # Imported inside the task, not at module scope. The dag-processor parses
        # this file every few seconds; a top-level import of a driver it does not
        # need makes every parse slower and turns a missing dependency into a
        # vanished DAG rather than a red task.
        import clickhouse_connect

        password = _require_env("CLICKHOUSE_PASSWORD")
        host = _require_env("CLICKHOUSE_HOST")
        port = int(os.environ.get("CLICKHOUSE_PORT", "8123"))
        user = os.environ.get("CLICKHOUSE_USER", "elt_writer")

        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            # See the note above on elt_writer's default database.
            database=CLICKHOUSE_DATABASE,
            connect_timeout=5,
            send_receive_timeout=30,
        )

        # Both DDL statements are IF NOT EXISTS and are safe on every run. They
        # are here so the DAG can rebuild its own target on a fresh ClickHouse,
        # not because the objects are expected to be missing — they were created
        # ahead of the first run.
        client.command(f"CREATE DATABASE IF NOT EXISTS {CLICKHOUSE_DATABASE}")
        client.command(
            f"""
            CREATE TABLE IF NOT EXISTS {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
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
            ORDER BY (ticker, ingested_at)
            """
        )

        # The retry guard. See the module docstring for why the window is the run
        # and not all of history.
        window_start = context["data_interval_start"].in_timezone("UTC").to_datetime_string()
        already = client.query(
            f"""
            SELECT count()
            FROM {CLICKHOUSE_DATABASE}.{CLICKHOUSE_TABLE}
            WHERE ticker = %(ticker)s
              AND api_timestamp = %(api_timestamp)s
              AND ingested_at >= %(window_start)s
            """,
            parameters={
                "ticker": row["ticker"],
                "api_timestamp": row["api_timestamp"],
                "window_start": window_start,
            },
        ).result_rows[0][0]

        if already:
            # Not a skip and not a failure. The run's job was to make sure this
            # observation is in the table; it is. Logging the reason matters more
            # than the exit state, because a silent no-op is indistinguishable
            # from a broken insert when someone reads this a month from now.
            print(
                f"Row for {row['ticker']} @ {row['api_timestamp']} was already ingested "
                f"in this run's window (>= {window_start}). This is a retry. Not inserting."
            )
            return

        columns = [
            "ticker",
            "current_price",
            "change",
            "change_pct",
            "high",
            "low",
            "open",
            "previous_close",
            "api_timestamp",
        ]
        # ingested_at is omitted on purpose so ClickHouse's DEFAULT now() supplies
        # it. That keeps the ingestion clock on the database, which is the only
        # clock every reader of this table shares.
        client.insert(
            table=CLICKHOUSE_TABLE,
            database=CLICKHOUSE_DATABASE,
            data=[[row[column] for column in columns]],
            column_names=columns,
        )
        print(f"Inserted {row['ticker']} @ {row['api_timestamp']} (price {row['current_price']}).")

    insert_aapl_quote(fetch_aapl_quote())


stock_market_landing()
