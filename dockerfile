# Base Airflow image
FROM apache/airflow:2.7.2-python3.9

USER root

# ----------------------------
# Install Java (for Spark)
# ----------------------------
RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# ----------------------------
# Install Spark
# ----------------------------
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -L -o /tmp/spark.tgz \
      https://downloads.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm /tmp/spark.tgz

# ----------------------------
# Python dependencies
# ----------------------------
# Copy requirements and install (don't reinstall Airflow)
COPY --chown=airflow:root requirements.txt /opt/airflow/
RUN pip install --no-cache-dir --upgrade pip wheel setuptools==66.1.1 && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# ----------------------------
# Airflow directories
# ----------------------------
RUN mkdir -p /opt/airflow/{dags,logs,plugins,include,data,dags/jars}

# ----------------------------
# Delta Lake jars
# ----------------------------
RUN curl -L -o /opt/airflow/dags/jars/delta-core_2.12-2.3.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar && \
    curl -L -o /opt/airflow/dags/jars/delta-storage-2.3.0.jar \
      https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar

# ----------------------------
# Copy configs & Spark jobs
# ----------------------------
COPY --chown=airflow:root airflow_settings.yaml /opt/airflow/
COPY --chown=airflow:root spark_job /opt/airflow/spark_job

# ----------------------------
# Environment variables
# ----------------------------
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow
WORKDIR /opt/airflow

# ----------------------------
# Fix permissions
# ----------------------------
RUN chown -R airflow:root /opt/airflow

USER airflow

# ----------------------------
# Entrypoint and CMD
# ----------------------------
ENTRYPOINT ["/entrypoint"]
CMD ["webserver"]
