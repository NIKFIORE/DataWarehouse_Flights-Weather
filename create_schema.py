from cassandra.cluster import Cluster
import time

def connect(host="cassandra", max_retries=10):
    for attempt in range(max_retries):
        try:
            cluster = Cluster([host])
            session = cluster.connect()
            session.default_timeout = 60  # timeout 60s per ogni query
            print("✅ Connesso a Cassandra")
            print("⏳ Attendo stabilizzazione Cassandra (15s)...")
            time.sleep(15)
            return cluster, session
        except Exception as e:
            print(f"⏳ Attendo Cassandra (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Cassandra")


cluster, session = connect()

# ===============================
# KEYSPACE
# ===============================

session.execute("""
CREATE KEYSPACE IF NOT EXISTS flight_db
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
""")
print("✅ Keyspace creato")

session.set_keyspace("flight_db")

# ===============================
# TABELLE DIMENSIONE
# ===============================

session.execute("""
CREATE TABLE IF NOT EXISTS airline (
    id_airline UUID PRIMARY KEY,
    name       TEXT
);
""")
print("✅ airline")

session.execute("""
CREATE TABLE IF NOT EXISTS aircraft (
    id_aircraft UUID PRIMARY KEY,
    tail_number TEXT,
    id_airline  UUID
);
""")
print("✅ aircraft")

session.execute("""
CREATE TABLE IF NOT EXISTS day (
    id_day      UUID PRIMARY KEY,
    day         INT,
    day_of_week TEXT,
    month       INT,
    year        INT
);
""")
print("✅ day")

session.execute("""
CREATE TABLE IF NOT EXISTS city_airport (
    id_city   UUID PRIMARY KEY,
    city_name TEXT
);
""")
print("✅ city_airport")

session.execute("""
CREATE TABLE IF NOT EXISTS airport (
    id_airport   UUID PRIMARY KEY,
    id_city      UUID,
    airport_name TEXT
);
""")
print("✅ airport")

session.execute("""
CREATE TABLE IF NOT EXISTS weather (
    id_weather       UUID PRIMARY KEY,
    city             TEXT,
    datetime_weather TIMESTAMP,
    weather_type     TEXT,
    wind_speed       DOUBLE,
    wind_direction   TEXT
);
""")
print("✅ weather")

session.execute("""
CREATE INDEX IF NOT EXISTS weather_city_idx
ON weather (city);
""")
print("✅ weather index")

session.execute("""
CREATE TABLE IF NOT EXISTS state_flight (
    id_state            UUID PRIMARY KEY,
    cancelled           BOOLEAN,
    diverted            BOOLEAN,
    cancellation_reason TEXT
);
""")
print("✅ state_flight")

session.execute("""
CREATE TABLE IF NOT EXISTS description_flight (
    id_description_flight UUID PRIMARY KEY,
    flight_number         TEXT,
    scheduled_departure   TIMESTAMP,
    scheduled_arrival     TIMESTAMP,
    scheduled_time        INT
);
""")
print("✅ description_flight")

session.execute("""
CREATE TABLE IF NOT EXISTS flight (
    id_flight              UUID PRIMARY KEY,
    id_day                 UUID,
    id_description_flight  UUID,
    id_airport_origin      UUID,
    id_airport_destination UUID,
    id_aircraft            UUID,
    id_state               UUID,
    departure_delay        INT,
    arrival_delay          INT,
    taxi_out               INT,
    taxi_in                INT,
    air_time               INT,
    elapsed_time           INT,
    scheduled_time         INT,
    distance               DOUBLE,
    air_system_delay       INT,
    security_delay         INT,
    airline_delay          INT,
    late_aircraft_delay    INT
);
""")
print("✅ flight")

print("\n🎯 Schema Cassandra creato correttamente.")
cluster.shutdown()