# Airflow scheduler image — Phase 1 §6.4.
#
# Airflow 3.3.1 is the TOP of the range Cosmos 1.14.x supports (2.9-2.11 and
# 3.0-3.3, per elt-pipeline-verification.md §4). Pinned to an exact patch, not
# `latest`: an image that silently moves is an image whose failures cannot be
# attributed to a commit.
#
# Build context is the REPOSITORY ROOT:
#   docker build -f airflow/Dockerfile .

FROM apache/airflow:3.3.1-python3.12

# The base image already runs as the `airflow` user. Do not switch to root to
# install; the image is built to be pip-installed into as `airflow`.

COPY airflow/requirements.txt /tmp/requirements.txt

# --constraint is not optional. Airflow publishes a constraints file per version
# and Python minor; without it, pip is free to resolve a provider that upgrades
# Airflow itself out from under the pin, and the failure appears at DAG import
# time rather than at build time.
RUN pip install --no-cache-dir \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-3.3.1/constraints-3.12.txt" \
      -r /tmp/requirements.txt

COPY airflow/dags/ /opt/airflow/dags/
