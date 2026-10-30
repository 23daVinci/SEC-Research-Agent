FROM apache/airflow:3.3.2-python3.12

# Must match the base image. Used to pick the matching constraints file.
ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

COPY requirements.txt /requirements.txt

# The constraints file pins every transitive dependency to a version the Airflow release was
# tested with, so adding our own packages can't silently break Airflow.
RUN pip install --no-cache-dir -r /requirements.txt \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"
