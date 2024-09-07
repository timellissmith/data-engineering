FROM python:3.10

LABEL maintainer="Tim Ellis-Smith"

ARG DBT_VERSION="1.2"

RUN apt-get update
RUN apt-get install -y git libpq-dev libblas-dev liblapack-dev gfortran libatlas-base-dev
RUN pip install --upgrade cffi
RUN pip install cryptography~=3.4
RUN pip install dbt-bigquery==${DBT_VERSION}
RUN mkdir -p /home/airflow/gcs/dags/dbt

WORKDIR /home/airflow/gcs/dags/dbt
ENTRYPOINT ["/bin/bash"]
