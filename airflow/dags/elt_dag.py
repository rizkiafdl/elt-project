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

import json
import logging
from datetime import datetime, timedelta

from airflow.exceptions import AirflowException
from airflow.providers.cncf.kubernetes.operators.pod import KubernetesPodOperator
from cosmos import DbtDag, ExecutionConfig, ProfileConfig, ProjectConfig, RenderConfig
from cosmos.constants import ExecutionMode, LoadMode, TestBehavior
from kubernetes.client import models as k8s

log = logging.getLogger(__name__)

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
DBT_RUNNER_IMAGE = "ghcr.io/rizkiafdl/dbt-runner@sha256:2fae76e887620f3ce58a56bef2f1bf1e2fdccfe1fa6bbc61fef752b0f040cc8f"  # ci:dbt-runner-digest

# ⚠️ WAS "dev" (DuckDB) UNTIL 2026-09-10. The DuckDB target was removed at Rizki's
# direction, so "prod" is not a promotion here — it is the only target that exists
# (source: dbt/profiles.yml, which now has exactly one output).
#
# 🚩 THIS STRING WAS INERT AND IS NOT INERT ANY MORE -- both halves of the warning
# below have now happened, so it is kept as the record of when. WAS: "no dbt pod has
# ever been spawned; §10.3 has not written `operator_args`, this DAG is paused, and
# its schedule is None". Since then §10.5 spawned real pods against the REAL
# warehouse, and §11.5 (2026-09-10) gave this DAG `schedule="5 * * * *"`. There is no
# longer a harmless target to fail into, and no longer a human deciding when to try.
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
# 🕰️ `stg_placeholder_events` WAS DELETED AT §8.4, in the commit that added the first
# real model. The measurement above is kept as written because it is the record of
# what was measured; do not go looking for that model. The shape it measured is
# unchanged -- today AFTER_EACH renders `stg_stock_quote.run` and
# `stg_stock_quote.test`.
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

# ── §10.4 — THE MANIFEST/IMAGE SKEW GUARD ──────────────────────────────────────
# THE PROBLEM THIS EXISTS FOR
#
# This DAG is rendered from `manifest.json` on a PVC, and executed by the
# `dbt-runner` image pinned above. Those two artifacts are built by ONE CI run from
# ONE commit, but they REACH THE CLUSTER BY DIFFERENT ROUTES, on different clocks:
#
#   manifest -> `manifest-sync` CronJob, `schedule: "17 * * * *"`  -> up to 60 min
#   image    -> Flux ImageRepository 5m + ImagePolicy 5m
#               + ImageUpdateAutomation 5m + Kustomization 10m      -> ~5-25 min
#   (source: homelab-infra/flux/manifest-sync/cronjob.yaml,
#            homelab-infra/flux/image-automation/*.yaml)
#
# So after a merge the two halves arrive minutes to an hour apart, and there are two
# skewed states. They are NOT symmetric:
#
#   * IMAGE AHEAD, manifest behind (the common case, because the image route is
#     faster). The DAG renders the OLD task list. A new model exists inside the
#     image and nothing calls it. Harmless, silent, and looks like "my model did
#     not deploy". -> WARN, do not raise.
#
#   * MANIFEST AHEAD, image behind (the dangerous case). The DAG renders a task for
#     a model the dbt-runner image does not contain, the pod runs
#     `dbt run --select fqn:...`, and dbt answers `Model ... not found`. THAT ERROR
#     IS A LIE: it reads like broken SQL, and the SQL is fine. -> RAISE.
#
# WHY THIS RAISES AT MODULE SCOPE AND NOT INSIDE A TASK
#
# §10.4 requires the failure to be LOUD: "the DAG raises at parse time, or the first
# task fails with a message naming both versions. Anything that renders a DAG which
# then fails per-model is not loud enough." Raising here means the dag-processor
# reports one import error naming both commits, and NO task graph is ever built --
# so there is nothing to mistake for a dbt problem.
#
# 🚩 A RAISE HERE IS NOT ALWAYS A FAULT. During the minutes when the manifest has
# landed and the image has not, the estate really IS in the broken state, and this
# guard is reporting it accurately. It clears itself when Flux rolls the image. The
# message says so, so nobody debugs a deploy that is merely in progress.
#
# ⚠️ THE TWO LINES BELOW ARE REWRITTEN BY CI. DO NOT EDIT THEM BY HAND.
# Same mechanism as the digest above: `.github/workflows/dbt-ci.yml` substitutes on
# the trailing marker comment, and a verification step fails the build if the
# substitution did not take. The placeholders are deliberately IMPOSSIBLE values --
# an all-zero SHA and CI run 0 -- so an un-rewritten DAG compares as older than every
# real manifest and fails LOUDLY here, rather than quietly running with a stale pin.
BUILD_SOURCE_SHA = "09d15ff3db74627f2b0ad07069168a9580c2c709"  # ci:build-source-sha
BUILD_CI_RUN = 29  # ci:build-ci-run

# The manifest side of the pair.
#
# 🚩 dbt STRIPS THE `DBT_ENV_CUSTOM_ENV_` PREFIX. The variable CI sets is
# `DBT_ENV_CUSTOM_ENV_GIT_SHA`; the key that lands in `metadata.env` is `GIT_SHA`.
# THE TWO NAMES ARE NOT THE SAME AND THIS FILE MUST USE THE SHORT ONE.
#
# This was written the wrong way round first, on the assumption that the full name
# survived, and shipped in CI run 23. Measured immediately afterwards against the
# published manifest:
#
#   "env": { "GIT_SHA": "557d4061...", "CI_RUN": "23" }
#
# (source: curl of the manifest-latest Release asset, 2026-09-10). The guard caught
# its own defect -- it saw neither key, concluded the manifest was unstamped, and
# raised -- which is the failure direction the placeholders were chosen to produce.
#
# ⚠️ The names below must stay in step with the `DBT_ENV_CUSTOM_ENV_*` variables on
# the `dbt` job in .github/workflows/dbt-ci.yml, MINUS THE PREFIX. Nothing checks
# that correspondence automatically; the workflow carries the matching warning.
MANIFEST_SHA_KEY = "GIT_SHA"
MANIFEST_RUN_KEY = "CI_RUN"

# What CI must set to produce those keys. Quoted here only so the error messages can
# name the variable a reader has to go and fix, not just the key that is missing.
CI_SHA_VAR = f"DBT_ENV_CUSTOM_ENV_{MANIFEST_SHA_KEY}"
CI_RUN_VAR = f"DBT_ENV_CUSTOM_ENV_{MANIFEST_RUN_KEY}"

# One command, quoted here so the error message can hand it over verbatim.
FORCE_SYNC = (
    "kubectl create job -n elt --from=cronjob/manifest-sync manifest-sync-manual"
)


def _assert_manifest_matches_image() -> None:
    """Raise at parse time if the manifest is NEWER than the pinned image.

    Reads nothing but the manifest already mounted for rendering, and compares two
    values that one CI run wrote into both artifacts. No network call, no registry
    lookup, no git history -- all three of those record the commit too, and none of
    them is reachable from a scheduler pod.
    """
    try:
        with open(MANIFEST_PATH) as fh:
            metadata = json.load(fh).get("metadata", {})
    except (OSError, ValueError):
        # Missing, unreadable or malformed manifest is NOT this guard's failure to
        # report. `RenderConfig(load_method=LoadMode.DBT_MANIFEST)` raises on the same
        # file moments from now, with a better message. Do not duplicate it.
        return

    env = metadata.get("env") or {}
    manifest_sha = env.get(MANIFEST_SHA_KEY)
    manifest_run = env.get(MANIFEST_RUN_KEY)

    if manifest_sha == BUILD_SOURCE_SHA:
        return

    # An UNSTAMPED manifest predates §10.4, so it is older than this image by
    # definition. This is the one-time bootstrap state on the deploy that ships this
    # guard, and it clears at the next `:17`. Force the sync to skip the wait.
    if not manifest_sha:
        raise AirflowException(
            "manifest/image skew (§10.4) -- the manifest on the PVC carries no "
            f"metadata.env[{MANIFEST_SHA_KEY!r}] (set in CI as {CI_SHA_VAR}), so it "
            "was built before this guard shipped and is "
            f"older than this image (commit {BUILD_SOURCE_SHA[:12]}, CI run "
            f"{BUILD_CI_RUN}, manifest generated_at={metadata.get('generated_at')}). "
            f"Force a manifest sync to clear it: {FORCE_SYNC}"
        )

    try:
        manifest_run_n = int(manifest_run)
    except (TypeError, ValueError):
        raise AirflowException(
            "manifest/image skew (§10.4) -- the manifest carries "
            f"{MANIFEST_SHA_KEY}={manifest_sha[:12]} but its {MANIFEST_RUN_KEY} is "
            f"{manifest_run!r}, which is not an integer. {CI_RUN_VAR} is what sets "
            "it. The two stamps are written "
            "by the same CI job and must both be present; a manifest with one and "
            "not the other means the workflow was edited incompletely."
        ) from None

    if manifest_run_n < BUILD_CI_RUN:
        # Image ahead. The safe direction: the task list is stale, not wrong.
        log.warning(
            "manifest/image skew (§10.4), SAFE DIRECTION -- the image is ahead of the "
            "manifest. Rendering the older task list. manifest: %s (CI run %d); "
            "image: %s (CI run %d). Any model added in the newer commit will not "
            "appear as a task until the manifest-sync CronJob next fires (:17). "
            "Force it with: %s",
            manifest_sha[:12], manifest_run_n,
            BUILD_SOURCE_SHA[:12], BUILD_CI_RUN,
            FORCE_SYNC,
        )
        return

    raise AirflowException(
        "manifest/image skew (§10.4) -- the manifest is NEWER than the dbt-runner "
        f"image this DAG pins. manifest: {manifest_sha[:12]} (CI run "
        f"{manifest_run_n}); image: {BUILD_SOURCE_SHA[:12]} (CI run {BUILD_CI_RUN}). "
        "Rendering now would create tasks for models the image does not contain, and "
        "each would fail inside dbt with a model-not-found that reads like broken "
        "SQL. Refusing to render instead. IF A DEPLOY IS IN PROGRESS THIS CLEARS "
        "ITSELF when Flux rolls the newer airflow image (~5-25 min: ImageRepository "
        "5m + ImagePolicy 5m + ImageUpdateAutomation 5m + Kustomization 10m). If it "
        "does NOT clear, the image half of the deploy is stuck -- check the Flux "
        "ImagePolicy and the airflow Kustomization, not this DAG and not dbt."
    )


_assert_manifest_matches_image()

elt_dag = DbtDag(
    dag_id="elt_project",
    # ✅ §11.5, 2026-09-10. WAS `schedule=None` ("manual trigger only, on purpose")
    # until the cadence could be defended. It can be now, and every minute below is
    # taken from a measurement rather than a preference:
    #
    #   :00  `stock_market_landing` fires (schedule="0 * * * *"). It is two plain
    #        tasks in the scheduler pod, no dbt and no pods to spawn, and it lands
    #        in ~5 s -- observed `ingested_at` 11:00:05 for the 11:00 run.
    #   :05  THIS DAG. Five minutes is ~60x the observed landing duration, so the
    #        transform reads a landing table the loader has finished writing.
    #   :17  `manifest-sync` CronJob replaces the manifest on the PVC
    #        (source: homelab-infra/flux/manifest-sync/cronjob.yaml).
    #
    # 🚩 :05 IS CHOSEN TO SIT BEFORE :17, NOT MERELY AFTER :00. A push landing
    # between :10 and :16 syncs a NEW manifest at :17 before Flux can roll the
    # matching image, and `_assert_manifest_matches_image()` then raises at PARSE for
    # up to ~25 minutes -- correctly, because the estate really is mismatched
    # (§10.4, findings topic 29 §1). Running at :05 uses a manifest that has been
    # settled for ~48 minutes and clears that window entirely. Scheduling at, say,
    # :20 or :30 would put the run inside the one window the guard is designed to
    # raise in, and would look like a flaky pipeline instead of a working guard.
    #
    # Hourly matches the loader by construction. `sources.yml` derives its freshness
    # thresholds from the SAME hourly cadence (warn_after 2h, error_after 6h), so
    # changing this schedule means changing those two numbers with it.
    schedule="5 * * * *",
    start_date=datetime(2026, 9, 9),
    # catchup=False is load-bearing, not tidiness: start_date is 2026-09-09 and this
    # DAG became scheduled on 2026-09-10, so catchup=True would queue ~24 backfill
    # runs at once, each spawning a pod per dbt node, against a warehouse whose
    # memory ceiling was only just pinned (§11.2).
    catchup=False,
    # 🚩 ONE RUN AT A TIME. The default is 16. Two concurrent runs would have two
    # dbt processes building the SAME marts in the SAME database -- `mart_stock_daily_ohlc`
    # and `mart_stock_quote_latest` are MergeTree tables that dbt rebuilds, not
    # appends to. If a run ever overruns its hour, the correct behaviour is to skip,
    # not to race. `stock_market_landing` sets this for the same reason.
    max_active_runs=1,
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


# ── §11.5 — THE SOURCE-FRESHNESS ALARM ─────────────────────────────────────────
# WHAT IT WATCHES. `sources.yml` declares the contract -- `warn_after: 2 hours`,
# `error_after: 6 hours` on `landing.stock_quote`, both derived from the loader's
# hourly schedule. Nothing evaluated that contract on a schedule until this task
# existed: §8.2 ran it once by hand, from a Job, and closed.
#
# 🚩 WHY IT IS NOT JUST `cmds: ["dbt"]` WITH `arguments: ["source", "freshness"]`.
# `dbt source freshness` EXITS 0 ON A WARN, and `--warn-error` does not change that
# -- both measured on 2026-09-10 against the real table (findings topic 40). A task
# that ran dbt directly would be green through the entire WARN band and would only
# turn red at `error_after`, six hours in, which is an outage rather than an early
# warning. `/usr/local/bin/freshness-gate` (dbt-runner/freshness-gate.py) runs the
# same dbt command and then reads the `status` field out of `target/sources.json`,
# which is the only artifact that carries it.
#
# 🚩 DELIBERATELY NOT UPSTREAM OF THE dbt TASKS. It has no dependency edge in either
# direction, so it runs beside them and never blocks them. A stale source does not
# make the transform wrong -- the models are a full rebuild from whatever `landing`
# holds, so a run on two-hour-old rows produces correct marts over slightly old
# data. Blocking would convert "the loader missed an hour" into "the warehouse
# stopped updating", which is a strictly worse outage than the one being reported.
# The task going red IS the alarm; the pipeline keeping going is the point.
#
# ⚠️ AND NOTHING WATCHES THE RED. Airflow shows a failed task and emails nobody:
# no alerting backend is configured anywhere in this estate, which Phase 1 recorded
# as an open gap and Phase 2 has only widened. "Alerting" here means the run is
# visibly failed to somebody who looks. Wiring it to something that pages is the
# first item of whatever observability work follows this phase.
with elt_dag:
    KubernetesPodOperator(
        task_id="source_freshness",
        name="source-freshness",
        image=DBT_RUNNER_IMAGE,
        # Overrides the image's `ENTRYPOINT ["dbt"]`. This is the ONLY setting that
        # differs from the dbt task pods; see the Dockerfile comment.
        cmds=["/usr/local/bin/freshness-gate"],
        arguments=["--target", DBT_TARGET, "--project-dir", DBT_PROJECT_PATH],
        namespace="elt",
        in_cluster=True,
        get_logs=True,
        image_pull_policy="IfNotPresent",
        # Same reasoning as OPERATOR_ARGS: keep a FAILED pod for inspection, because
        # this pod failing is the alarm and its logs are the report.
        on_finish_action="delete_succeeded_pod",
        # 🚩 FRESH V1EnvVar OBJECTS, NOT `OPERATOR_ARGS["env_vars"]`. Cosmos rewrites
        # `operator.env_vars` in place when it builds its own pods (the flattening
        # documented above), so handing it the same list objects would couple this
        # task to that rewrite. The VALUES are the same three literals on purpose --
        # the address and the credential still arrive through `env_from` below and
        # are still absent from this public repository.
        env_vars=[
            k8s.V1EnvVar(name="CLICKHOUSE_PORT", value="8123"),
            k8s.V1EnvVar(name="CLICKHOUSE_USER", value="elt_writer"),
            k8s.V1EnvVar(name="CLICKHOUSE_DATABASE", value="elt"),
        ],
        env_from=OPERATOR_ARGS["env_from"],
        # A freshness check is one `SELECT max(ingested_at)`. If it has not answered
        # in five minutes the warehouse is not answering, which is itself the alarm.
        execution_timeout=timedelta(minutes=5),
    )
