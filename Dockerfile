FROM apache/airflow:2.10.2-python3.11

USER airflow

COPY requirements-docker.txt /tmp/requirements-docker.txt
RUN pip install --no-cache-dir -r /tmp/requirements-docker.txt
