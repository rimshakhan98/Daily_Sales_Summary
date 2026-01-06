# Base Airflow image
FROM apache/airflow:2.7.2-python3.10

# ----------------------------
# System dependencies (root)
# ----------------------------
USER root

RUN apt-get update && \
    apt-get install -y openjdk-11-jdk curl procps && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# ----------------------------
# Install Spark
# ----------------------------
ENV SPARK_VERSION=3.3.2
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH

RUN curl -fL \
  https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz \
  -o /tmp/spark.tgz && \
  tar -xzf /tmp/spark.tgz -C /opt/ && \
  mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
  rm /tmp/spark.tgz

# ----------------------------
# Airflow directories
# ----------------------------
RUN mkdir -p /opt/airflow/{dags,logs,plugins,include,data,spark_job/jars}

# ----------------------------
# Copy project files
# ----------------------------
COPY --chown=airflow:root airflow_settings.yaml /opt/airflow/
COPY --chown=airflow:root spark_job /opt/airflow/spark_job
COPY --chown=airflow:root requirements.txt /opt/airflow/

# ----------------------------
# Python dependencies (airflow user ONLY)
# ----------------------------
USER airflow

RUN pip install --no-cache-dir --upgrade pip wheel setuptools==66.1.1 && \
    pip install --no-cache-dir -r /opt/airflow/requirements.txt

# ----------------------------
# Environment
# ----------------------------
ENV AIRFLOW_HOME=/opt/airflow
ENV PYTHONPATH=/opt/airflow
WORKDIR /opt/airflow

# ----------------------------
# Default command
# ----------------------------
CMD ["webserver"]
