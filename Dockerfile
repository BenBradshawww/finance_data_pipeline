FROM apache/airflow:latest

USER root

RUN apt-get update && \
    apt-get install -y default-jdk && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME /usr/lib/jvm/java-11-openjdk-arm64

USER airflow

COPY requirements.txt /opt/airflow/requirements.txt

RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

WORKDIR /opt/airflow

ENTRYPOINT ["/bin/bash", "-c"]
CMD ["airflow scheduler"]