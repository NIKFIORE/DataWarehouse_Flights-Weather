import pandas as pd
from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time
import uuid
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# ===============================
# CONFIGURAZIONE
# ===============================

FLIGHTS_CSV = "Dataset/flights.csv"
WEATHER_DESC_CSV = "Dataset/weather_description.csv"
WIND_DIR_CSV = "Dataset/wind_direction.csv"
WIND_SPEED_CSV = "Dataset/wind_speed.csv"

CHUNK_SIZE = 500
STREAM_INTERVAL = 2  # secondi

KAFKA_BOOTSTRAP = "kafka:9092"
CASSANDRA_HOST = "cassandra"
KEYSPACE = "flight_db"

# ===============================
# CASSANDRA SETUP
# ===============================

def get_cassandra_session():
    """Connette a Cassandra con retry automatico."""
    max_retries = 10
    for attempt in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            print("✅ Connesso a Cassandra")
            return session
        except Exception as e:
            print(f"⏳ Cassandra non pronta (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Cassandra dopo i tentativi massimi")


# ===============================
# KAFKA SETUP
# ===============================

def get_producer():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
            )
            print("✅ Producer Kafka connesso")
            return producer
        except Exception as e:
            print(f"⏳ Kafka non pronto (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Kafka")


def get_consumer():
    max_retries = 10
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                "flights_topic",
                "weather_topic",
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="etl-group",
            )
            print("✅ Consumer Kafka connesso")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non pronto per consumer (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Kafka (consumer)")


# ===============================
# TRASFORMAZIONE WEATHER (wide → long)
# ===============================

def melt_weather_chunk(desc_chunk, dir_chunk, speed_chunk):
    """
    Trasforma i 3 dataframe meteo dal formato wide al formato long:
        datetime | city | weather_description | wind_direction | wind_speed
    """
    # Melt weather_description
    desc_long = desc_chunk.melt(id_vars=["datetime"], var_name="city", value_name="weather_description")

    # Melt wind_direction
    dir_long = dir_chunk.melt(id_vars=["datetime"], var_name="city", value_name="wind_direction")

    # Melt wind_speed
    speed_long = speed_chunk.melt(id_vars=["datetime"], var_name="city", value_name="wind_speed")

    # Join sui 3 dataframe
    weather_long = desc_long.merge(dir_long, on=["datetime", "city"], how="outer")
    weather_long = weather_long.merge(speed_long, on=["datetime", "city"], how="outer")

    # Rimuovi righe completamente vuote
    weather_long.dropna(subset=["weather_description", "wind_direction", "wind_speed"], how="all", inplace=True)

    weather_long["datetime"] = pd.to_datetime(weather_long["datetime"])

    return weather_long


# ===============================
# STREAMING FUNCTION (PRODUCER)
# ===============================

def stream_data(producer):
    print("🚀 Avvio streaming...")

    flights_reader    = pd.read_csv(FLIGHTS_CSV,       chunksize=CHUNK_SIZE)
    weather_desc_reader = pd.read_csv(WEATHER_DESC_CSV, chunksize=CHUNK_SIZE)
    wind_dir_reader   = pd.read_csv(WIND_DIR_CSV,      chunksize=CHUNK_SIZE)
    wind_speed_reader = pd.read_csv(WIND_SPEED_CSV,    chunksize=CHUNK_SIZE)

    for f_chunk, wd_chunk, dir_chunk, ws_chunk in zip(
        flights_reader,
        weather_desc_reader,
        wind_dir_reader,
        wind_speed_reader,
    ):
        # --- FLIGHTS ---
        print(f"\n📦 Invio {len(f_chunk)} FLIGHT records")
        for record in f_chunk.to_dict(orient="records"):
            producer.send("flights_topic", record)

        # --- WEATHER (unpivot prima di inviare) ---
        weather_long = melt_weather_chunk(wd_chunk, dir_chunk, ws_chunk)
        print(f"🌤 Invio {len(weather_long)} WEATHER records (formato long)")
        for record in weather_long.to_dict(orient="records"):
            producer.send("weather_topic", record)

        producer.flush()
        print("✅ Chunk inviato. Attendo prossimo intervallo...\n")
        time.sleep(STREAM_INTERVAL)

    print("🎯 Streaming completato!")


# ===============================
# PREPARED STATEMENTS CASSANDRA
# ===============================

def prepare_statements(session):
    stmts = {}

    stmts["insert_airline"] = session.prepare("""
        INSERT INTO airline (id_airline, name) VALUES (?, ?)
        IF NOT EXISTS
    """)

    stmts["insert_aircraft"] = session.prepare("""
        INSERT INTO aircraft (id_aircraft, tail_number, id_airline) VALUES (?, ?, ?)
        IF NOT EXISTS
    """)

    stmts["insert_day"] = session.prepare("""
        INSERT INTO day (id_day, day, day_of_week, month, year) VALUES (?, ?, ?, ?, ?)
        IF NOT EXISTS
    """)

    stmts["insert_weather"] = session.prepare("""
        INSERT INTO weather (id_weather, weather_type, wind_speed, wind_direction, city, datetime_weather)
        VALUES (?, ?, ?, ?, ?, ?)
    """)

    stmts["insert_city"] = session.prepare("""
        INSERT INTO city_airport (id_city, city_name) VALUES (?, ?)
        IF NOT EXISTS
    """)

    stmts["insert_airport"] = session.prepare("""
        INSERT INTO airport (id_airport, id_city, airport_name) VALUES (?, ?, ?)
        IF NOT EXISTS
    """)

    stmts["insert_state"] = session.prepare("""
        INSERT INTO state_flight (id_state, cancelled, diverted, cancellation_reason)
        VALUES (?, ?, ?, ?)
    """)

    stmts["insert_description"] = session.prepare("""
        INSERT INTO description_flight (id_description_flight, flight_number, scheduled_departure, scheduled_arrival, scheduled_time)
        VALUES (?, ?, ?, ?, ?)
    """)

    stmts["insert_flight"] = session.prepare("""
        INSERT INTO flight (
            id_flight, id_day, id_description_flight,
            id_airport_origin, id_airport_destination,
            id_aircraft, id_state,
            departure_delay, arrival_delay,
            taxi_out, taxi_in, air_time, elapsed_time,
            scheduled_time, distance,
            air_system_delay, security_delay, airline_delay,
            late_aircraft_delay
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """)

    return stmts


# ===============================
# LOOKUP CACHE (evita duplicati)
# ===============================

_cache = {
    "airlines": {},   # name -> uuid
    "aircrafts": {},  # tail_number -> uuid
    "days": {},       # (year, month, day) -> uuid
    "cities": {},     # city_name -> uuid
    "airports": {},   # airport_code -> uuid
}

DAY_OF_WEEK_MAP = {1: "Monday", 2: "Tuesday", 3: "Wednesday",
                   4: "Thursday", 5: "Friday", 6: "Saturday", 7: "Sunday"}


def safe_int(val, default=0):
    try:
        v = int(val)
        return None if pd.isna(val) else v
    except (TypeError, ValueError):
        return default


def safe_float(val, default=0.0):
    try:
        return float(val)
    except (TypeError, ValueError):
        return default


def safe_str(val, default=""):
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val)


# ===============================
# INSERT HELPERS
# ===============================

def upsert_airline(session, stmts, name):
    name = safe_str(name, "UNKNOWN")
    if name not in _cache["airlines"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_airline"], (new_id, name))
        _cache["airlines"][name] = new_id
    return _cache["airlines"][name]


def upsert_aircraft(session, stmts, tail_number, id_airline):
    tail_number = safe_str(tail_number, "UNKNOWN")
    if tail_number not in _cache["aircrafts"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_aircraft"], (new_id, tail_number, id_airline))
        _cache["aircrafts"][tail_number] = new_id
    return _cache["aircrafts"][tail_number]


def upsert_day(session, stmts, year, month, day, day_of_week):
    key = (year, month, day)
    if key not in _cache["days"]:
        new_id = uuid.uuid4()
        dow_str = DAY_OF_WEEK_MAP.get(int(day_of_week), "Unknown")
        session.execute(stmts["insert_day"], (new_id, int(day), dow_str, int(month), int(year)))
        _cache["days"][key] = new_id
    return _cache["days"][key]


def upsert_city(session, stmts, city_name):
    city_name = safe_str(city_name, "UNKNOWN")
    if city_name not in _cache["cities"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_city"], (new_id, city_name))
        _cache["cities"][city_name] = new_id
    return _cache["cities"][city_name]


def upsert_airport(session, stmts, airport_code, id_city):
    airport_code = safe_str(airport_code, "UNKNOWN")
    if airport_code not in _cache["airports"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_airport"], (new_id, id_city, airport_code))
        _cache["airports"][airport_code] = new_id
    return _cache["airports"][airport_code]


# ===============================
# PROCESS MESSAGES
# ===============================

def process_flight(session, stmts, record):
    """Trasforma e inserisce un record di volo in Cassandra."""
    try:
        # Airline
        id_airline = upsert_airline(session, stmts, record.get("AIRLINE"))

        # Aircraft
        id_aircraft = upsert_aircraft(session, stmts, record.get("TAIL_NUMBER"), id_airline)

        # Day
        id_day = upsert_day(
            session, stmts,
            record.get("YEAR"), record.get("MONTH"),
            record.get("DAY"), record.get("DAY_OF_WEEK")
        )

        # Airports (origin / destination)
        origin_code = safe_str(record.get("ORIGIN_AIRPORT"), "UNKNOWN")
        dest_code   = safe_str(record.get("DESTINATION_AIRPORT"), "UNKNOWN")

        id_city_origin = upsert_city(session, stmts, origin_code)
        id_city_dest   = upsert_city(session, stmts, dest_code)

        id_airport_origin = upsert_airport(session, stmts, origin_code, id_city_origin)
        id_airport_dest   = upsert_airport(session, stmts, dest_code, id_city_dest)

        # State flight
        id_state = uuid.uuid4()
        session.execute(stmts["insert_state"], (
            id_state,
            bool(record.get("CANCELLED", 0)),
            bool(record.get("DIVERTED", 0)),
            safe_str(record.get("CANCELLATION_REASON")),
        ))

        # Description flight
        id_description = uuid.uuid4()
        session.execute(stmts["insert_description"], (
            id_description,
            safe_str(record.get("FLIGHT_NUMBER")),
            None,   # scheduled_departure (timestamp completo non disponibile nel CSV)
            None,   # scheduled_arrival
            safe_int(record.get("SCHEDULED_TIME")),
        ))

        # Flight centrale
        id_flight = uuid.uuid4()
        session.execute(stmts["insert_flight"], (
            id_flight,
            id_day,
            id_description,
            id_airport_origin,
            id_airport_dest,
            id_aircraft,
            id_state,
            safe_int(record.get("DEPARTURE_DELAY")),
            safe_int(record.get("ARRIVAL_DELAY")),
            safe_int(record.get("TAXI_OUT")),
            safe_int(record.get("TAXI_IN")),
            safe_int(record.get("AIR_TIME")),
            safe_int(record.get("ELAPSED_TIME")),
            safe_int(record.get("SCHEDULED_TIME")),
            safe_float(record.get("DISTANCE")),
            safe_int(record.get("AIR_SYSTEM_DELAY")),
            safe_int(record.get("SECURITY_DELAY")),
            safe_int(record.get("AIRLINE_DELAY")),
            safe_int(record.get("LATE_AIRCRAFT_DELAY")),
        ))

    except Exception as e:
        print(f"❌ Errore insert flight: {e} | record: {record}")


def process_weather(session, stmts, record):
    """Inserisce un record meteo (già in formato long) in Cassandra."""
    try:
        id_weather = uuid.uuid4()
        dt = record.get("datetime")
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)

        session.execute(stmts["insert_weather"], (
            id_weather,
            safe_str(record.get("weather_description")),
            safe_float(record.get("wind_speed")),
            safe_str(record.get("wind_direction")),
            safe_str(record.get("city")),
            dt,
        ))
    except Exception as e:
        print(f"❌ Errore insert weather: {e} | record: {record}")


# ===============================
# CONSUMER FUNCTION
# ===============================

def consume_data(session, stmts):
    consumer = get_consumer()
    print("👂 Consumer in ascolto...\n")

    for message in consumer:
        topic = message.topic
        record = message.value

        if topic == "flights_topic":
            process_flight(session, stmts, record)
        elif topic == "weather_topic":
            process_weather(session, stmts, record)


# ===============================
# MAIN
# ===============================

def main():
    print("🔵 ETL Pipeline avviata")

    # Attendi e connetti Cassandra
    session = get_cassandra_session()
    stmts   = prepare_statements(session)

    # Connetti producer
    producer = get_producer()

    # Avvia producer e consumer in parallelo
    producer_thread = threading.Thread(target=stream_data, args=(producer,))
    consumer_thread = threading.Thread(target=consume_data, args=(session, stmts), daemon=True)

    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    # Il consumer è daemon: si ferma quando il main thread termina
    print("🏁 Pipeline completata.")


if __name__ == "__main__":
    main()