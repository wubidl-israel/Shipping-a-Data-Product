FROM python:3.12-slim

RUN pip install --upgrade pip && \
    pip install dagster dagster-webserver dagster-postgres dagster-docker

ENV DAGSTER_HOME=/opt/dagster/dagster_home
WORKDIR $DAGSTER_HOME

COPY dagster.yaml workspace.yaml $DAGSTER_HOME
