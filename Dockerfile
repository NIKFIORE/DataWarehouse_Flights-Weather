FROM python:3.11-slim

# Java è necessario per PySpark
RUN apt-get update && apt-get install -y \
    default-jdk \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/default-java
ENV PATH="${JAVA_HOME}/bin:${PATH}"

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Scarica il connettore Spark-Cassandra e Spark-BigQuery
RUN curl -L -o /opt/spark-cassandra-connector.jar \
    https://repo1.maven.org/maven2/com/datastax/spark/spark-cassandra-connector_2.12/3.5.0/spark-cassandra-connector_2.12-3.5.0.jar

RUN curl -L -o /opt/spark-bigquery-connector.jar \
    https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.36.1.jar

COPY . .

CMD ["python", "ETL.py"]
