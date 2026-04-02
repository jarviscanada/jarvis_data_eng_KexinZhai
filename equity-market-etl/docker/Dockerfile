# docker/Dockerfile
FROM apache/airflow:2.8.1-python3.11


USER root

# Java runtime
RUN apt-get update \
  && apt-get install -y --no-install-recommends default-jre-headless \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*



USER airflow

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    exchange-calendars
