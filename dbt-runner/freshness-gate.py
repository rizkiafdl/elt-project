#!/usr/bin/env python3
"""Phase 2 §11.5 -- make a dbt source-freshness WARN fail loudly.

WHY THIS FILE EXISTS AT ALL
---------------------------
`sources.yml` declares a freshness contract on `landing.stock_quote`:
`warn_after: 2 hours`, `error_after: 6 hours`. Both numbers are derived from the
loader's hourly schedule, and the WARN threshold is the one that matters -- it is
the first sign that `stock_market_landing` has stopped landing rows. Six hours is
already an outage.

    THE PROBLEM: `dbt source freshness` EXITS 0 ON WARN.
    Only `error_after` turns anything red, so the WARN threshold, which is the
    early warning the contract exists for, is invisible to anything that reads an
    exit code -- which is everything: Airflow, a CronJob, a shell `&&`.

MEASURED 2026-09-10, NOT ASSUMED (findings topic 40). A probe pod tightened
`warn_after` to one minute in its own container filesystem, against the real
table:

    dbt source freshness                 -> "WARN in 0.03s", exit 0
    dbt --warn-error source freshness    -> "WARN in 0.06s", exit 0
    sources.json results[0].status       -> "warn"

The second line is the finding. `--warn-error` is the obvious one-flag fix and it
DOES NOT WORK for source freshness -- it promotes dbt's own warning *events*, and a
freshness WARN is a result *status*, not a warning event. Anyone who reaches for
that flag will watch it pass and conclude the alarm is armed. It is not.

So the status is read from the artifact, which is the only place that carries it.

WHAT THIS EXITS
---------------
    0   every source checked, every one of them `pass`
    1   any source `warn`, `error`, or `runtime error`
    1   dbt wrote no readable sources.json, whatever its own exit code was
    1   dbt checked NOTHING (empty results) -- a check that checked nothing must
        never report success; that is a `--select` that matched no source, and it
        would otherwise be a permanently green alarm

USAGE
    freshness-gate [any args to pass through to `dbt source freshness`]
e.g. freshness-gate --target prod --project-dir /usr/local/dbt/elt_project
"""

import json
import os
import subprocess
import sys

# dbt writes both artifacts under DBT_TARGET_PATH, which dbt-runner/Dockerfile
# points at /tmp so the image works with a non-root user (findings topic 25).
TARGET_PATH_VAR = "DBT_TARGET_PATH"
ARTIFACT = "sources.json"

# The one status that means "this source is inside its contract". Everything else
# -- "warn", "error", "runtime error" -- is the alarm.
PASS = "pass"


def main(argv: list[str]) -> int:
    target_path = os.environ.get(TARGET_PATH_VAR)
    if not target_path:
        print(
            f"freshness-gate: FAIL -- ${TARGET_PATH_VAR} is not set, so there is no "
            "way to find the artifact that carries the freshness status. The "
            "dbt-runner image sets it; a pod that overrides the environment "
            "wholesale will have dropped it.",
            file=sys.stderr,
        )
        return 1

    artifact = os.path.join(target_path, ARTIFACT)

    # Delete any artifact left by an earlier invocation BEFORE running dbt. A pod
    # is fresh so there is normally none, but if dbt crashes before writing, a
    # stale file would be read as though it described this run -- an alarm that
    # reports yesterday's freshness is worse than no alarm.
    try:
        os.remove(artifact)
    except FileNotFoundError:
        pass
    except OSError as exc:
        print(f"freshness-gate: FAIL -- cannot clear {artifact}: {exc}", file=sys.stderr)
        return 1

    cmd = ["dbt", "source", "freshness", *argv]
    print(f"freshness-gate: running: {' '.join(cmd)}", flush=True)
    dbt_exit = subprocess.call(cmd)
    print(f"freshness-gate: dbt exited {dbt_exit}", flush=True)

    try:
        with open(artifact) as fh:
            results = json.load(fh)["results"]
    except (OSError, ValueError, KeyError) as exc:
        print(
            f"freshness-gate: FAIL -- dbt exited {dbt_exit} and left no readable "
            f"{artifact} ({type(exc).__name__}: {exc}). Read dbt's own output above; "
            "this usually means it never reached the warehouse at all.",
            file=sys.stderr,
        )
        return 1

    if not results:
        print(
            f"freshness-gate: FAIL -- dbt checked NO sources. {artifact} has an empty "
            "`results` list, which means the selection matched nothing. This exits "
            "non-zero on purpose: an alarm wired to a selector that matches nothing "
            "is green forever.",
            file=sys.stderr,
        )
        return 1

    failed = []
    for result in results:
        status = result.get("status")
        name = result.get("unique_id", "<unknown source>")
        age_s = result.get("max_loaded_at_time_ago_in_s")
        age = f"{age_s / 3600:.2f}h" if isinstance(age_s, (int, float)) else "unknown"
        criteria = result.get("criteria") or {}
        line = (
            f"freshness-gate: {str(status).upper():<13} {name} "
            f"age={age} max_loaded_at={result.get('max_loaded_at')} "
            f"warn_after={criteria.get('warn_after')} "
            f"error_after={criteria.get('error_after')}"
        )
        print(line, flush=True)
        if status != PASS:
            failed.append((name, status, age))

    if failed:
        print(
            f"freshness-gate: FAIL -- {len(failed)} of {len(results)} source(s) are "
            "outside their freshness contract: "
            + "; ".join(f"{n} is {s} at {a}" for n, s, a in failed)
            + ". THE LOADER IS THE FIRST THING TO CHECK, NOT dbt: freshness is "
            "measured on `ingested_at`, so a WARN means rows stopped arriving, not "
            "that a transform is broken. Look at the `stock_market_landing` DAG.",
            file=sys.stderr,
        )
        return 1

    if dbt_exit != 0:
        # Every source passed but dbt still failed -- so the failure is dbt's own
        # (a connection, a parse, a missing grant). Do not mask it behind a green
        # alarm; hand back its exit code unchanged.
        print(
            f"freshness-gate: FAIL -- every source passed its contract, but dbt "
            f"itself exited {dbt_exit}. That is a dbt failure, not a freshness "
            "failure. Passing its exit code through unchanged.",
            file=sys.stderr,
        )
        return dbt_exit

    print(
        f"freshness-gate: OK -- {len(results)} source(s) checked, all inside their "
        "freshness contract."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
