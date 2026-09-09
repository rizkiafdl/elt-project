# elt-project

Application source for the homelab ELT pipeline: the dbt project, the Airflow DAG, the
image definitions, and the GitHub Actions workflows that test and publish them.

This repo is **public** and it is where all CI runs. The infrastructure that builds the
machines lives separately in the private `homelab-infra` repo, which Flux reconciles.
CI here never writes back to that repo — it pushes images to ghcr and publishes a
`manifest.json` Release asset, and the cluster **pulls** both. Nothing reaches into the
homelab, because under CGNAT nothing can.

## Layout

| Path | What |
|---|---|
| `dbt/` | The dbt project. Baked into the runner image, not mounted at runtime. |
| `dbt/profiles.yml` | **One** target: `prod` (ClickHouse). The DuckDB `dev` target was removed 2026-09-10. |
| `airflow/dags/` | Cosmos `DbtDag`. Phase 2. |
| `dbt-runner/` | Dockerfile. Phase 1 §6.4. |
| `.github/workflows/` | CI. Phase 1 §6.2–§6.5. |

## One target, and what that costs

`prod` is **ClickHouse**, and since 2026-09-10 it is the **only** target. Its host, user
and password come from environment variables with **no defaults**, supplied in-cluster
from a Kubernetes Secret.

⚠️ **There used to be a DuckDB `dev` target, and removing it changed what CI means.**
DuckDB was in-process and file-backed — no server, no credential — which is what let CI
run models and tests for real inside GitHub's cloud. That is gone.

**What CI does now:** `dbt deps`, then `dbt parse`, then a check that the manifest is not
empty. It publishes `manifest.json` and builds the two images. **It executes no model and
runs no test.**

**What CI cannot do, and why it is not a configuration gap:** GitHub's runner has no route
into this estate. It is behind CGNAT with no inbound address, and ClickHouse additionally
accepts connections from a single private address — the cluster node's — enforced by `ufw`
and by the database user's own host restriction. There is no reachable warehouse from CI,
so there is nothing to build against.

🚩 **`dbt compile` does not work here either, and that catches people out.** compile opens
a connection. Against an unreachable warehouse it fails with
`Database Error ... Connection refused`. **`dbt parse` does not connect** — that is why it
is the command CI uses.

**So a green CI run proves less than it used to.** It proves the project parses, that every
`ref`, `source`, macro and test resolves, and that the manifest has models in it. It does
**not** prove any model produces correct SQL. The first thing that executes a model is a
task pod in the cluster.

🚩 **One safety property was lost with the `dev` target.** "An unqualified `dbt run` cannot
touch production, because the default target is harmless" is no longer true — there is no
harmless target. What replaces it is the network: only the cluster node can reach the
warehouse at all. That is the stronger guard, but it is no longer visible in
`profiles.yml`.

## Local setup

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -r dbt/requirements.txt

cd dbt
export DBT_PROFILES_DIR=.

# profiles.yml has no defaults for these, on purpose -- this repository is public.
# dbt cannot even LOAD the profile without them, and the failure happens at
# profile-render time, which reads like a broken profiles.yml rather than a missing
# variable. `dbt parse` never connects, so placeholders are fine for parsing.
export CLICKHOUSE_HOST=local.invalid
export CLICKHOUSE_USER=parse-only
export CLICKHOUSE_ELT_WRITER_PASSWORD=parse-only

dbt deps
dbt parse            # resolves the project and writes target/manifest.json
```

`dbt parse` writes `dbt/target/`, which is gitignored and disposable.

⚠️ **`dbt build`, `dbt run`, `dbt test` and `dbt compile` all need a reachable
ClickHouse**, and a laptop does not have one — the database accepts connections from the
cluster node and nowhere else. To run them for real you need an SSH tunnel into the estate
and the real credentials. There is no local-only path any more; that is what the DuckDB
target used to provide.
