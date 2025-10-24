FROM apache/airflow:3.0.6

USER root
RUN apt-get update && apt-get install -y docker-cli

COPY requirements.txt /requirements.txt
USER airflow
RUN pip install --no-cache-dir -r /requirements.txt
