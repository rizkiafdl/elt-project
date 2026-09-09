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
| `dbt/profiles.yml` | Two targets: `dev` (DuckDB) and `prod` (ClickHouse). |
| `airflow/dags/` | Cosmos `DbtDag`. Phase 2. |
| `dbt-runner/` | Dockerfile. Phase 1 §6.4. |
| `.github/workflows/` | CI. Phase 1 §6.2–§6.5. |

## Two targets, and why

`dev` is **DuckDB** — in-process, file-backed, no server and no credential. GitHub's
runner has no route into the homelab, so DuckDB is the only target CI can select. It is
the default target, so an unqualified `dbt run` can never touch production.

`prod` is **ClickHouse**, reachable only from inside the estate. Its host, user and password
come from environment variables with **no defaults**, supplied in-cluster from a Secret.
It is first exercised in Phase 2 step 11.

A green CI run therefore proves the project is *coherent*, not that it runs on
ClickHouse. dbt SQL is not fully portable between adapters. That gap is what step 11 is
for.

## Local setup

```bash
uv venv --python 3.12 .venv
source .venv/bin/activate
uv pip install -r dbt/requirements.txt

cd dbt
export DBT_PROFILES_DIR=.
dbt build            # runs the models, then the tests, against DuckDB
```

`dbt build` writes `dbt/dev.duckdb` and `dbt/target/`. Both are gitignored and
disposable.
