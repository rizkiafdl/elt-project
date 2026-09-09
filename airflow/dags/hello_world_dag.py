"""Hello-world DAG — a deliberate end-to-end probe of the CI/CD loop.

⚠️ THROWAWAY. This DAG exists to prove the delivery chain, not to do work. Delete
it once the chain is trusted; it is tracked as a cleanup item in
``artifacts/working-items/phase-2/index.md`` beside the two placeholder dbt models.

WHAT THIS PROBE MEASURES
------------------------
The chain from an edit on this file to a task running in k3s has six hops, and a
break in any one of them looks the same from the Airflow UI (the DAG is simply
absent). This DAG isolates the chain because it depends on nothing else:

1. ``git push`` to ``rizkiafdl/elt-project`` on ``main``
2. GitHub Actions ``dbt CI`` — job ``dbt`` must pass before job ``images`` starts
   (source: ``.github/workflows/dbt-ci.yml``, ``needs: dbt``)
3. job ``images`` builds ``airflow/Dockerfile``, whose last line is
   ``COPY airflow/dags/ /opt/airflow/dags/`` — THE DAG IS BAKED INTO THE IMAGE,
   not synced from git. There is no git-sync sidecar in this deployment.
4. the image is pushed to ``ghcr.io/rizkiafdl/airflow`` and a digest is printed
   to the run summary
5. ⚠️ A HUMAN COPIES THAT DIGEST into ``flux/airflow/helmrelease.yaml`` in
   ``rizkiafdl/homelab-infra`` and pushes. This is the ONE manual hop. Flux image
   automation is off and both deploy keys are read-only, so nothing writes it
   back (Phase 1 findings topic 9). A DAG change is therefore TWO commits.
6. Flux → helm-controller → new pods → the dag-processor parses this file

Because this DAG imports only the Airflow SDK, a failure at step 6 means the
delivery chain is broken. It can never mean "Cosmos or dbt is misconfigured",
which is exactly what ``elt_dag.py`` cannot tell you on its own.

WHY THE AUTHORING API IS ``airflow.sdk``
----------------------------------------
Airflow 3 moved DAG authoring into the Task SDK. ``from airflow.sdk import dag,
task`` is the supported path on 3.3.1; the Airflow 2 imports
(``airflow.decorators``, ``airflow.operators.python``) still resolve but are the
legacy surface. Verified inside the running dag-processor pod, not from docs::

    kubectl exec -n elt <dag-processor pod> -c dag-processor -- \
      python -c "from airflow.sdk import DAG, dag, task; print(DAG)"
    # -> <class 'airflow.sdk.definitions.dag.DAG'>

HOW TO RE-RUN THE PROBE
-----------------------
Bump ``HELLO_MARKER`` below, push, wait for CI, bump the digest in
``helmrelease.yaml``, push. Then trigger the DAG from the UI and read the task
log: the marker printed there is proof of WHICH build is serving traffic. A stale
marker means the digest bump did not land — the pods kept the old image, which is
the failure mode that ``UpgradeSucceeded`` hides (it is sticky from the previous
upgrade; findings topic 11).
"""

from __future__ import annotations

import os
import platform
import sys
from datetime import datetime

from airflow.sdk import dag, task

# Bump this by hand on every re-test. It is the only value in the log that proves
# the running image was built from the latest commit rather than a cached one.
HELLO_MARKER = "v2"


@dag(
    dag_id="hello_world",
    # No schedule. This probe must never run on its own — it is triggered by hand
    # from the UI, exactly like elt_project (which carries its own §11.5 flag to
    # replace schedule=None with a real cron).
    schedule=None,
    start_date=datetime(2026, 9, 9),
    catchup=False,
    tags=["probe", "ci-cd", "throwaway"],
    # A delivery probe that retries hides the very failure it is there to show.
    default_args={"retries": 0},
    doc_md=__doc__,
)
def hello_world():
    @task
    def hello() -> str:
        """Print the marker and the identity of the pod that ran it."""
        # LocalExecutor runs this inside the scheduler pod, so the hostname below
        # is the scheduler's. That is intentional: elt_project's tasks run in
        # their own pods under ExecutionMode.KUBERNETES, so a difference between
        # the two logs tells you which executor actually served the task.
        lines = [
            f"hello world from the homelab -- marker {HELLO_MARKER}",
            f"pod hostname : {platform.node()}",
            f"python       : {sys.version.split()[0]}",
            f"airflow home : {os.environ.get('AIRFLOW_HOME', '<unset>')}",
            f"executor     : {os.environ.get('AIRFLOW__CORE__EXECUTOR', '<unset>')}",
        ]
        for line in lines:
            print(line)
        return HELLO_MARKER

    @task
    def confirm(marker: str) -> None:
        """Fail loudly if XCom did not carry the value between tasks.

        Two tasks rather than one, because a single task proves only that the DAG
        parsed. A dependency proves the scheduler queued, ran and passed a value
        between task instances -- the part that needs the metadata database, which
        is the PostgreSQL sub-chart installed at §9.4.
        """
        if marker != HELLO_MARKER:
            raise ValueError(f"expected marker {HELLO_MARKER!r}, received {marker!r}")
        print(f"xcom round trip ok -- marker {marker}")

    confirm(hello())


hello_world()
