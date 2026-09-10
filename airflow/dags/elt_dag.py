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
from kubernetes.client import models as k8s

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
DBT_RUNNER_IMAGE = "ghcr.io/rizkiafdl/dbt-runner@sha256:db492c45d628d61e0b45d759e0f05f9e3162ac211fc4470c61efd23008c319c7"  # ci:dbt-runner-digest

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

# ── §10.3 — POD WIRING FOR THE dbt-runner TASK PODS ────────────────────────────
# ✅ DECIDED AND VERIFIED 2026-09-10. Every key here was checked against the INSTALLED
# Cosmos 1.15.1 by calling `build_kube_args` and `build_pod_request_obj` on a rendered
# task in the running scheduler pod and inspecting the resulting `V1Pod` -- not assumed
# from the constructor signature. Confirmed on that build:
#   container[0].command == ["dbt"]
#   container[0].args    == ["run", "--select", "fqn:...", "--profile", "elt_project",
#                             "--target", "prod", "--project-dir",
#                             "/usr/local/dbt/elt_project"]
# i.e. NOT "dbt dbt run ...". `cmds: ["dbt"]` is what makes that branch fire in
# `build_kube_args` (see the module docstring and §10.1's finding, topic 19).
#
# 🚩 THIS FILE CARRIES NO PRIVATE ADDRESS AND NO CREDENTIAL. `elt-project` is public.
# CLICKHOUSE_HOST is sourced from a ConfigMap (`clickhouse-conn`) that lives in
# `homelab-infra` -- the same seam that repository's `flux/airflow/helmrelease.yaml`
# already documents at §13.5 for the Airflow chart's own containers. A dbt-runner pod is
# NOT one of those containers -- it is a fresh pod the scheduler spawns through the
# Kubernetes API at run time via `KubernetesPodOperator`, and it inherits none of the
# chart's env. That gap is what §10.3 exists to close.
#
# 🔀 REVISED 2026-09-10 AFTER §10.5's FIRST REAL RUN. Both indirect values were wired as
# `env_vars` entries carrying a `valueFrom`, which is the obvious reading of the
# KubernetesPodOperator API and is WRONG under Cosmos -- it drops them without a word.
# They now travel via `env_from`. See the block on `env_vars` below.
#
# PORT, USER and DATABASE are plain, non-sensitive literals (a port number, a username,
# a schema name) and are written here directly -- only the address and the password are
# routed indirectly, because only those two are the kind of fact this repository must
# never carry.
#
# ⚠️ NAME RECONCILED, NOT RENAMED. `dbt/profiles.yml` reads
# `env_var('CLICKHOUSE_ELT_WRITER_PASSWORD')`. The HelmRelease injects the same secret
# into the *scheduler* container under the different name `CLICKHOUSE_PASSWORD`. §10.3
# does not touch either name. Because `envFrom` names the variable after the SECRET KEY,
# the reconciliation now lives in the Secret itself: `clickhouse-elt-writer` carries the
# one value under two keys -- `password` for the HelmRelease's `secretKeyRef`, and
# `CLICKHOUSE_ELT_WRITER_PASSWORD` for this pod's `envFrom`. One value, three consumer
# names, each spelled where its consumer expects it.
#
# 🚩 `on_finish_action="delete_succeeded_pod"`, NOT the chart default of deleting every
# pod. §10.5's gate explicitly needs to inspect a FAILED pod
# (`kubectl describe pod`, `kubectl logs`) before deciding what went wrong. A pod that
# succeeded is deleted as usual; one that failed is left for inspection until removed by
# hand.
OPERATOR_ARGS = {
    "image": DBT_RUNNER_IMAGE,
    # 🚩 REQUIRED. The dbt-runner image's ENTRYPOINT is already `dbt`
    # (dbt-runner/Dockerfile:62). Without this, Cosmos 1.15.1 preserves the default
    # (unset `cmds`) and the pod runs `dbt dbt run ...` -- see the block comment above.
    "cmds": ["dbt"],
    "image_pull_policy": "IfNotPresent",
    "namespace": "elt",
    "in_cluster": True,
    "get_logs": True,
    "on_finish_action": "delete_succeeded_pod",
    # ── env: LITERALS ONLY. Anything with a `valueFrom` MUST go in `env_from` below.
    #
    # 🚩 COSMOS 1.15.1 SILENTLY DISCARDS `valueFrom` FROM `env_vars`. This is not a
    # style preference; it is a measured defect, and it cost §10.5 a full run to find.
    # `cosmos/operators/_k8s_common.py::_build_env_vars` flattens whatever the user
    # passed down to a plain `{name: value}` dict and rebuilds it:
    #
    #     for ev in existing_env_vars:
    #         env_vars_dict[ev.name] = ev.value      # reads ONLY .value
    #     return convert_env_vars(env_vars_dict)     # rebuilds V1EnvVar(name, value)
    #
    # `build_kube_args` then assigns the result back over `operator.env_vars`, so a
    # `V1EnvVar(name=..., value_from=V1EnvVarSource(...))` reaches the pod as
    # `{name: ..., value: None, value_from: None}`. Kubernetes renders that as an EMPTY
    # STRING, dbt's `env_var()` returns "", and the clickhouse adapter falls back to its
    # own default host — `localhost:8123` — where it fails with `Connection refused`.
    # NOTHING IN THE LOG SAYS AN ENV VAR WAS DROPPED. The pod looks correctly configured
    # right up until the connection error names a host nobody configured.
    # Detail: findings topic 26.
    "env_vars": [
        k8s.V1EnvVar(name="CLICKHOUSE_PORT", value="8123"),
        k8s.V1EnvVar(name="CLICKHOUSE_USER", value="elt_writer"),
        k8s.V1EnvVar(name="CLICKHOUSE_DATABASE", value="elt"),
    ],
    # ── env_from: THE ADDRESS AND THE CREDENTIAL, THE ONLY TWO FACTS THIS PUBLIC
    # REPOSITORY MUST NEVER CARRY.
    #
    # ✅ VERIFIED ON THE RUNNING SCHEDULER, not assumed: Cosmos rewrites `env_vars` and
    # never reads or reassigns `env_from`, so these references survive the flattening
    # above and land on the container intact. Probe: attach `env_from`, call
    # `build_kube_args` then `build_pod_request_obj`, and read
    # `container.env_from` off the V1Pod that would be submitted (findings topic 26 §5).
    #
    # ⚠️ `envFrom` HAS NO KEY SELECTOR. It imports EVERY key in the object and USES THE
    # KEY NAME AS THE ENV VAR NAME. That is why:
    #   * `clickhouse-conn` (homelab-infra) has its key named `CLICKHOUSE_HOST`, not `host`
    #   * `clickhouse-elt-writer` carries a key named `CLICKHOUSE_ELT_WRITER_PASSWORD`,
    #     which is the name `dbt/profiles.yml` already reads. The Secret's original
    #     `password` key stays for the HelmRelease, which injects it into the CHART's
    #     containers under the third name `CLICKHOUSE_PASSWORD` (§13.5).
    # Renaming either key silently reintroduces the `localhost` failure, because a
    # missing key produces no error here — only a differently-named variable.
    #
    # Both objects are namespaced to `elt` and are resolved by the kubelet at pod start,
    # so this file names them and never their contents.
    "env_from": [
        k8s.V1EnvFromSource(config_map_ref=k8s.V1ConfigMapEnvSource(name="clickhouse-conn")),
        k8s.V1EnvFromSource(secret_ref=k8s.V1SecretEnvSource(name="clickhouse-elt-writer")),
    ],
}

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
    operator_args=OPERATOR_ARGS,
)
