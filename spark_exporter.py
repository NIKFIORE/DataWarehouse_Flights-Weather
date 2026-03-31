"""
spark_exporter.py
Legge le tabelle Cassandra (keyspace: flight_db) tramite PySpark
e le esporta direttamente su BigQuery senza passare per GCS.
"""

import os
import time
from pyspark.sql import SparkSession

# ── Configurazione ──────────────────────────────────────────────────────────
GCP_PROJECT_ID      = os.environ.get("GCP_PROJECT_ID", "flight-warehouse")
GCP_CREDENTIALS     = os.environ.get("GCP_CREDENTIALS_PATH", "/app/flight-warehouse-f8b21f5a9cec.json")
BQ_DATASET          = os.environ.get("BQ_DATASET", "flight_db")
CASSANDRA_HOST      = os.environ.get("CASSANDRA_HOST", "cassandra")
CASSANDRA_KEYSPACE  = "flight_db"

# Tabelle da esportare: (nome_cassandra, nome_bigquery)
TABLES = [
    "airline",
    "aircraft",
    "day",
    "city_airport",
    "airport",
    "state_flight",
    "description_flight",
    "weather",
    "flight",
    "flight_weather",
]

SPARK_CASSANDRA_JAR = "/opt/spark-cassandra-connector.jar"
SPARK_BQ_JAR        = "/opt/spark-bigquery-connector.jar"

# ── Attendi Cassandra ────────────────────────────────────────────────────────
def wait_for_cassandra(host: str, max_retries: int = 20, delay: int = 10):
    from cassandra.cluster import Cluster
    from cassandra.cluster import NoHostAvailable
    for attempt in range(1, max_retries + 1):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.execute("SELECT now() FROM system.local")
            cluster.shutdown()
            print(f"✅ Cassandra raggiungibile dopo {attempt} tentativi")
            return
        except Exception:
            print(f"⏳ Attendo Cassandra... ({attempt}/{max_retries})")
            time.sleep(delay)
    raise RuntimeError("❌ Cassandra non raggiungibile dopo i tentativi massimi")


# ── Crea SparkSession ────────────────────────────────────────────────────────
def create_spark() -> SparkSession:
    spark = (
        SparkSession.builder
        .appName("CassandraToBigQuery")
        .master("local[*]")
        .config("spark.jars", f"{SPARK_CASSANDRA_JAR},{SPARK_BQ_JAR}")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.cassandra.connection.port", "9042")
        # BigQuery — credenziali
        .config("credentialsFile", GCP_CREDENTIALS)
        .config("parentProject", GCP_PROJECT_ID)
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ── Esporta singola tabella ──────────────────────────────────────────────────
def export_table(spark: SparkSession, table: str):
    bq_table = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{table}"
    print(f"📤 Esportazione: {CASSANDRA_KEYSPACE}.{table} → {bq_table}")

    try:
        df = (
            spark.read
            .format("org.apache.spark.sql.cassandra")
            .options(keyspace=CASSANDRA_KEYSPACE, table=table)
            .load()
        )

        count = df.count()
        if count == 0:
            print(f"⚠️  Tabella {table} vuota, skip.")
            return

        print(f"   Righe lette da Cassandra: {count:,}")

        (
            df.write
            .format("com.google.cloud.spark.bigquery")
            .option("table", bq_table)
            .option("writeMethod", "direct")       # nessun bucket GCS necessario
            .mode("overwrite")
            .save()
        )

        print(f"   ✅ {table} → BigQuery OK ({count:,} righe)")

    except Exception as e:
        print(f"   ❌ Errore su {table}: {e}")


# ── Entry point ──────────────────────────────────────────────────────────────
def main():
    print("🚀 Spark Exporter avviato")
    print(f"   Progetto GCP : {GCP_PROJECT_ID}")
    print(f"   Dataset BQ   : {BQ_DATASET}")
    print(f"   Cassandra    : {CASSANDRA_HOST}")

    wait_for_cassandra(CASSANDRA_HOST)

    spark = create_spark()

    for table in TABLES:
        export_table(spark, table)

    spark.stop()
    print("\n🎉 Esportazione completata! Dati disponibili su BigQuery.")


if __name__ == "__main__":
    main()
