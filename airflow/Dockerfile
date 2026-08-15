FROM apache/airflow:3.3.1-python3.11

COPY airflow/requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt

# dbt and Great Expectations get their own virtualenvs instead of sharing
# Airflow's. Not fastidiousness - a shared environment is unsatisfiable:
# every dbt-databricks release caps databricks-sdk below the 0.128.0 that the
# ingest DAG's files.upload needs (1.9.4 pins ==0.17.0 exactly), and 1.10+
# additionally demands databricks-sql-connector>=4.0 against the 3.6.0 that
# fixes pyarrow<17 for this image. A venv is the cheapest isolation boundary
# that doesn't require mounting the Docker socket into the scheduler.
#
# Installed from each service's own pinned requirements.txt, so the versions
# are single-sourced with what dbt/run.ps1 uses on the host rather than being
# duplicated (and drifting) here. Build context is the repo root - see the
# `dockerfile:` key in docker-compose.yml.
USER root
COPY dbt/requirements.txt /tmp/dbt-requirements.txt
COPY quality/requirements.txt /tmp/quality-requirements.txt
RUN /usr/python/bin/python3.11 -m venv /opt/dbt-venv \
    && /opt/dbt-venv/bin/pip install --no-cache-dir -r /tmp/dbt-requirements.txt \
    && /usr/python/bin/python3.11 -m venv /opt/ge-venv \
    && /opt/ge-venv/bin/pip install --no-cache-dir -r /tmp/quality-requirements.txt \
    && rm /tmp/dbt-requirements.txt /tmp/quality-requirements.txt
USER airflow
