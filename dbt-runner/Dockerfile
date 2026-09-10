# dbt-runner — Phase 1 §6.4.
#
# Carries dbt-core, both adapters, AND the dbt project itself. The project is
# baked in rather than mounted because Cosmos in ExecutionMode.KUBERNETES has no
# ProfileMapping and no runtime git-sync, so the pod must already contain what it
# runs (source: secondary/elt-pipeline-plan/elt-pipeline-verification.md §4, §8).
#
# The consequence is deliberate and worth stating: a model change requires an
# image rebuild. That coupling is the price of running Cosmos on Kubernetes.
#
# Build context is the REPOSITORY ROOT, not this directory:
#   docker build -f dbt-runner/Dockerfile .

FROM python:3.12-slim AS builder

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /build

# Only the pin list, so this layer is cached until the pins actually change.
# Copying the whole project here would rebuild the dependency layer on every
# model edit.
COPY dbt/requirements.txt ./requirements.txt

RUN python -m venv /opt/venv \
 && /opt/venv/bin/pip install --no-cache-dir -r requirements.txt


FROM python:3.12-slim

# The dbt project path. This literal must match `dbt_project_path` in the Cosmos
# ExecutionConfig and anything §7 writes. A mismatch surfaces in Phase 2 as a pod
# that starts and immediately cannot find its project.
ARG DBT_HOME=/usr/local/dbt/elt_project

ENV PATH=/opt/venv/bin:$PATH \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    DBT_PROFILES_DIR=${DBT_HOME} \
    # dbt writes logs and compiled SQL at run time. Point both at /tmp so the
    # image works with a read-only root filesystem and a non-root user, which is
    # how it will be run in k3s.
    DBT_LOG_PATH=/tmp/dbt-logs \
    DBT_TARGET_PATH=/tmp/dbt-target

COPY --from=builder /opt/venv /opt/venv
COPY dbt/ ${DBT_HOME}/

WORKDIR ${DBT_HOME}

# Resolve dbt_utils at build time from the committed package-lock.yml, so the
# running pod never needs to reach dbt Hub. A pod that fetches packages at
# start-up fails whenever the network or the Hub is having a bad day.
#
# `dbt deps` runs here as root -- `USER dbt` is set below, not above. Any dbt
# invocation initialises the event logger, and the logger CREATES DBT_LOG_PATH
# (/tmp/dbt-logs) and opens dbt.log inside it. Created by root, that directory is
# mode 755 root:root. The pod then runs as uid 1000, dbt's setup_event_logger
# finds the directory already present so it does not recreate it, and
# RotatingFileHandler fails with `PermissionError: [Errno 13] ... /tmp/dbt-logs/
# dbt.log` -- dbt aborts before it opens a single database connection
# (source: secondary/elt-pipeline-plan/phase-2-execution-findings.md topic 25).
#
# Delete the build-time log and target directories so the non-root runtime user
# recreates them itself, owned by uid 1000, on the first `dbt` call in the pod.
RUN dbt deps --no-version-check \
 && rm -rf /tmp/dbt-logs /tmp/dbt-target

# Non-root. uid 1000 is a plain choice, not inherited from anything.
RUN useradd --create-home --uid 1000 dbt \
 && chown -R dbt:dbt ${DBT_HOME}
USER dbt

ENTRYPOINT ["dbt"]
CMD ["--version"]
