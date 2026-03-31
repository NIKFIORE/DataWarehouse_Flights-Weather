# ===============================
# cassandra_client.py — Connessione e prepared statements
# ===============================

import time
from cassandra.cluster import Cluster
from etl.config import CASSANDRA_HOST, KEYSPACE


def get_session(max_retries: int = 10):
    """Connette a Cassandra con retry automatico."""
    for attempt in range(max_retries):
        try:
            cluster = Cluster([CASSANDRA_HOST])
            session = cluster.connect(KEYSPACE)
            session.default_timeout = 60
            print("✅ Connesso a Cassandra")
            return session
        except Exception as e:
            print(f"⏳ Cassandra non pronta (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Cassandra")


def prepare_statements(session) -> dict:
    """Prepara e restituisce tutti gli statement CQL."""
    return {
        "insert_airline": session.prepare("""
            INSERT INTO airline (id_airline, name)
            VALUES (?, ?) IF NOT EXISTS
        """),
        "insert_aircraft": session.prepare("""
            INSERT INTO aircraft (id_aircraft, tail_number, id_airline)
            VALUES (?, ?, ?) IF NOT EXISTS
        """),
        "insert_day": session.prepare("""
            INSERT INTO day (id_day, day, day_of_week, month, year)
            VALUES (?, ?, ?, ?, ?) IF NOT EXISTS
        """),
        "insert_city": session.prepare("""
            INSERT INTO city_airport (id_city, city_name)
            VALUES (?, ?) IF NOT EXISTS
        """),
        "insert_airport": session.prepare("""
            INSERT INTO airport (id_airport, id_city, airport_name)
            VALUES (?, ?, ?) IF NOT EXISTS
        """),
        "insert_state": session.prepare("""
            INSERT INTO state_flight (id_state, cancelled, diverted, cancellation_reason)
            VALUES (?, ?, ?, ?)
        """),
        "insert_description": session.prepare("""
            INSERT INTO description_flight
                (id_description_flight, flight_number, scheduled_departure, scheduled_arrival, scheduled_time)
            VALUES (?, ?, ?, ?, ?)
        """),
        "insert_flight": session.prepare("""
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
        """),
        "insert_weather": session.prepare("""
            INSERT INTO weather
                (id_weather, weather_type, wind_speed, wind_direction, city, datetime_weather)
            VALUES (?, ?, ?, ?, ?, ?)
        """),
        "insert_flight_weather": session.prepare("""
            INSERT INTO flight_weather (
                id_fw, flight_date, origin_iata, origin_city,
                departure_delay, arrival_delay, distance,
                weather_type, wind_speed, wind_direction, cancelled
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """),
    }
