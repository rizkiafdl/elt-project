"""Cosmos DAG for the elt_project dbt project — Phase 2 §9.6.

WHAT MAKES THIS DAG EXIST
-------------------------
Nothing in this file lists models. The task graph is rendered from a pre-compiled
dbt ``manifest.json`` that a CronJob syncs onto a PVC once an hour, mounted here
read-only at ``/opt/manifests``. Adding a model to the dbt project and pushing it
makes a new task appear with no edit to this file. That property is the entire
reason the manifest route was chosen.

The three literals below are fixed by Phase 1 §7.1 and are not free choices:
namespace ``elt``, claim ``dbt-manifest``, path ``/opt/manifests/manifest.json``.

API CORRECTION, VERIFIED AGAINST THE INSTALLED COSMOS (1.14.2)
--------------------------------------------------------------
``manifest_path`` belongs on ``ProjectConfig``, NOT on ``RenderConfig``.
``RenderConfig`` carries ``load_method=LoadMode.DBT_MANIFEST``. An earlier draft of
the plan had this the other way round; do not reintroduce it from memory. Checked
with ``inspect.signature`` inside the running dag-processor pod, not from docs.

WHY dbt_project_path IS None ON ProjectConfig BUT SET ON ExecutionConfig
-----------------------------------------------------------------------
Two different machines need two different things:

* The **scheduler / dag-processor** only RENDERS. It reads the manifest and never
  runs dbt, so it needs no dbt project on disk — and the Airflow image contains
  none (verified: no ``dbt_project.yml`` anywhere in that image). ``project_name``
  is what lets ProjectConfig work without a project directory.
* The **task pods** EXECUTE. They run the ``dbt-runner`` image, which bakes the
  project in at ``/usr/local/dbt/elt_project`` (source: ``dbt-runner/Dockerfile``,
  ``ARG DBT_HOME``). That is the path ExecutionConfig points at.

Baking rather than mounting is forced, not preferred: Cosmos in
``ExecutionMode.KUBERNETES`` supports no ``ProfileMapping`` and injects no
git-sync, so the pod must already contain what it runs.
"""

from __future__ import annotations

from datetime import datetime

from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior

# ── FIXED LITERALS ─────────────────────────────────────────────────────────────
# Phase 1 §7.1. A mismatch here surfaces as a DAG that vanishes from the UI with an
# import error that reads like a Cosmos bug.
MANIFEST_PATH = "/opt/manifests/manifest.json"

# Inside the dbt-runner image, not this one. Must match dbt-runner/Dockerfile's
# DBT_HOME exactly.
DBT_PROJECT_PATH = "/usr/local/dbt/elt_project"

# dbt_project.yml: name -> elt_project, profile -> elt_project.
DBT_PROJECT_NAME = "elt_project"
DBT_PROFILE_NAME = "elt_project"

# `dev` is the DuckDB target. The `prod` ClickHouse target is promoted at §11.5,
# which is also when its credentials arrive and §9.3's "Connections declared in
# Git" condition starts to bite. DuckDB needs no credential, so nothing is
# declared here yet.
DBT_TARGET = "dev"

# ⚠️ OWNED BY §10.1, NOT BY THIS FILE. §10.1 decides the execution mode and settles
# the astronomer-cosmos <1.15 pin as one decision. KUBERNETES is the plan's choice
# and §10.1's current recommendation, so it is the honest default to render
# against — but the pod details (image, digest, env, RBAC, operator_args) are
# §10.3's, and are deliberately absent here. §9 renders; §10 executes.
EXECUTION_MODE = ExecutionMode.KUBERNETES

# ⚠️ THE SINGLE LARGEST LEVER ON POD COUNT, SET DELIBERATELY RATHER THAN INHERITED.
# Measured on the placeholder manifest inside the dag-processor pod:
#     AFTER_EACH -> 2 tasks   ['stg_placeholder_events.run', 'stg_placeholder_events.test']
#     AFTER_ALL  -> 2 tasks   ['stg_placeholder_events_run', 'elt_project_test']
#     NONE       -> 1 task    ['stg_placeholder_events_run']
# Cosmos reports `Total nodes: 6` for all three. Tests do NOT become one task each:
# AFTER_EACH emits ONE `test` task per model that runs every test on that model in
# a single dbt invocation.
# AFTER_EACH is kept because it fails per model: a broken test on one model blocks
# that model's downstream and nothing else. AFTER_ALL costs the same two tasks but
# loses that isolation.
TEST_BEHAVIOR = TestBehavior.AFTER_EACH

elt_dag = DbtDag(
    dag_id="elt_project",
    # ⚠️ MANUAL TRIGGER ONLY, ON PURPOSE. How often the source data changes decides
    # this number, and §8.1 has not answered that yet. Defaulting to @daily would
    # invent a cadence nobody can defend later.
    # 🚩 §11.6's exit gate requires a run that was SCHEDULED, not hand-triggered, so
    # this must be replaced at §11.5. Recorded so it is not lost between §9 and §11.
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    tags=["dbt", "cosmos", "elt"],
    default_args={"retries": 0},
    doc_md=__doc__,
    project_config=ProjectConfig(
        # No dbt project on this machine — see the module docstring.
        dbt_project_path=None,
        manifest_path=MANIFEST_PATH,
        project_name=DBT_PROJECT_NAME,
    ),
    render_config=RenderConfig(
        # Explicit, never AUTOMATIC. AUTOMATIC would fall back to running `dbt ls`
        # when the manifest is missing, which turns a broken mount into a slow DAG
        # instead of a loud failure.
        load_method=LoadMode.DBT_MANIFEST,
        test_behavior=TEST_BEHAVIOR,
    ),
    profile_config=ProfileConfig(
        profile_name=DBT_PROFILE_NAME,
        target_name=DBT_TARGET,
        # Path inside the dbt-runner image. dbt-runner/Dockerfile sets
        # DBT_PROFILES_DIR to DBT_HOME and copies dbt/ (profiles.yml included) there.
        profiles_yml_filepath=f"{DBT_PROJECT_PATH}/profiles.yml",
    ),
    execution_config=ExecutionConfig(
        execution_mode=EXECUTION_MODE,
        dbt_project_path=DBT_PROJECT_PATH,
    ),
)
