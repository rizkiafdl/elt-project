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

API CORRECTION, VERIFIED AGAINST THE INSTALLED COSMOS
----------------------------------------------------
``manifest_path`` belongs on ``ProjectConfig``, NOT on ``RenderConfig``.
``RenderConfig`` carries ``load_method=LoadMode.DBT_MANIFEST``. An earlier draft of
the plan had this the other way round; do not reintroduce it from memory. Checked
with ``inspect.signature`` inside the running dag-processor pod, not from docs.

First verified on Cosmos 1.14.2. §10.1 moved the pin to ``>=1.15.1,<1.16`` on
2026-09-10 and re-checked the ``ProjectConfig`` / ``RenderConfig`` /
``ExecutionConfig`` dataclass fields field-by-field across the two releases: the
diff is ADDITIVE ONLY, and every keyword this file passes still exists and still
lives on the same class. The reason for the bump is a pod-``command`` bug, not an
API need — see ``airflow/requirements.txt``.

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

# ── THE dbt-runner IMAGE — §10.2 ───────────────────────────────────────────────
# WHAT THIS LINE IS, AND WHY IT LOOKS LIKE THAT
#
# §10.2 decided where the dbt-runner digest lives: HERE, in the DAG, in the public
# `elt-project` repository. The alternative was `homelab-infra`, and it was rejected
# for a concrete reason — the digest is consumed by `operator_args` on this DAG
# object, so putting it in the other repository would mean the Airflow image and the
# value it needs ship from two different commits that nothing keeps in step. That is
# exactly the manifest/image skew hazard §10.4 exists to make loud, and there is no
# reason to create a second instance of it on purpose.
#
# ⚠️ THE LINE BELOW IS REWRITTEN BY CI. DO NOT EDIT IT BY HAND.
# `.github/workflows/dbt-ci.yml` builds dbt-runner first, substitutes the digest that
# build produced into this exact line, builds the airflow image FROM THE REWRITTEN
# TREE, and only then commits the rewrite back to `main`. The order matters and is
# not an implementation detail:
#
#   * The image is built from the rewritten file, so the running DAG always carries
#     the digest of the dbt-runner image built from ITS OWN commit. The two images
#     can no longer drift, because one build produces both.
#   * The commit back is bookkeeping — it keeps `main` honest about what is running.
#     If that push loses a race and is retried away, the IMAGE IS STILL CORRECT; only
#     git lags. That is the safe direction to fail in.
#
# The commit back does not start a second CI run. A push authenticated with the
# built-in `GITHUB_TOKEN` does not trigger workflows, and the commit message also
# carries `[skip ci]` as a second guard, so this cannot become a build loop.
#
# The marker comment at the end of the line is the substitution anchor. Removing it
# does not break the build — it breaks the REWRITE, silently, and CI then fails at
# its own verification step rather than publishing an image pinned to the
# placeholder.
#
# 🚩 THE PLACEHOLDER IS ALL ZEROS ON PURPOSE. It is not a valid digest and cannot be
# pulled. If the rewrite ever no-ops and the verification is bypassed, a task pod
# fails at `ImagePullBackOff` — loud and immediate — instead of quietly running some
# older dbt-runner that happens to still be in the node's image cache.
#
# ⚠️ NOT CONSUMED YET. §10.3 is what puts this into `operator_args`; until then the
# constant is written, verified and committed but nothing reads it. That seam is
# deliberate: §10.2 owns how the value ARRIVES, §10.3 owns how the pod USES it.
DBT_RUNNER_IMAGE = "ghcr.io/rizkiafdl/dbt-runner@sha256:62742bfdef7b380b9b42ec3065ad98ba448f3f6cf2658a5410e1000f8788a7a7"  # ci:dbt-runner-digest

# ⚠️ WAS "dev" (DuckDB) UNTIL 2026-09-10. The DuckDB target was removed at Rizki's
# direction, so "prod" is not a promotion here — it is the only target that exists
# (source: dbt/profiles.yml, which now has exactly one output).
#
# 🚩 THIS STRING IS INERT TODAY AND WILL NOT STAY INERT. No dbt pod has ever been
# spawned: §10.3 has not written `operator_args`, this DAG is paused, and its
# schedule is None. So changing it writes nothing to ClickHouse right now. The
# moment §10.5 spawns its first pod, that pod talks to the REAL warehouse — there
# is no longer a harmless target to fail into.
#
# 🚩 CONSEQUENCE FOR THE TRIPWIRES. §11.1 (off-box backups) and §11.2 (the
# ClickHouse memory mis-sizing) were written as preconditions to §11.5, because
# §11.5 was where production was first touched. Removing DuckDB moves that moment
# earlier, to §10.5. The tripwires did not move; the thing they guard did.
DBT_TARGET = "prod"

# ✅ DECIDED AT §10.1 ON 2026-09-10 — route B. This is no longer a placeholder.
# `ExecutionMode.KUBERNETES` is kept (one pod per dbt node, which is what the plan
# asked for) and the astronomer-cosmos pin moves to `>=1.15.1,<1.16` in the same
# decision, because the two are one question: `ExecutionMode.WATCHER` only exists
# above the old bound, and the pod-`command` bug that route A shared is only
# FIXABLE above it.
#
# The pod details — image, env, RBAC, `operator_args` — are still §10.3's and are
# still deliberately absent here. §9 renders; §10 executes.
#
# 🚩 §10.3 MUST PASS `cmds: ["dbt"]` IN `operator_args`. The dbt-runner image has
# `ENTRYPOINT ["dbt"]`, and Cosmos otherwise puts the executable in `arguments`,
# producing `dbt dbt run --select ...` in the pod. Detail: findings topic 19.
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
