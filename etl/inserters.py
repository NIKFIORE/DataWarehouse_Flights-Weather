# ===============================
# inserters.py — Inserimento dati in Cassandra
# ===============================

import uuid
from datetime import datetime, date
from etl.transforms import safe_int, safe_float, safe_str
from etl.config import DAY_OF_WEEK_MAP, IATA_TO_CITY

# Cache in memoria per evitare insert duplicati su entità dimensione
_cache: dict = {
    "airlines":  {},
    "aircrafts": {},
    "days":      {},
    "cities":    {},
    "airports":  {},
}

# Cache meteo: (city, "YYYY-MM-DD") -> {weather_type, wind_speed, wind_direction}
_weather_cache: dict = {}


# -------------------------------------------------------
# UPSERT DIMENSIONI
# -------------------------------------------------------

def upsert_airline(session, stmts, name: str) -> uuid.UUID:
    name = safe_str(name, "UNKNOWN")
    if name not in _cache["airlines"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_airline"], (new_id, name))
        _cache["airlines"][name] = new_id
    return _cache["airlines"][name]


def upsert_aircraft(session, stmts, tail_number: str, id_airline: uuid.UUID) -> uuid.UUID:
    tail_number = safe_str(tail_number, "UNKNOWN")
    if tail_number not in _cache["aircrafts"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_aircraft"], (new_id, tail_number, id_airline))
        _cache["aircrafts"][tail_number] = new_id
    return _cache["aircrafts"][tail_number]


def upsert_day(session, stmts, year, month, day, day_of_week) -> uuid.UUID:
    key = (int(year), int(month), int(day))
    if key not in _cache["days"]:
        new_id  = uuid.uuid4()
        dow_str = DAY_OF_WEEK_MAP.get(int(day_of_week), "Unknown")
        session.execute(stmts["insert_day"], (new_id, int(day), dow_str, int(month), int(year)))
        _cache["days"][key] = new_id
    return _cache["days"][key]


def upsert_city(session, stmts, city_name: str) -> uuid.UUID:
    city_name = safe_str(city_name, "UNKNOWN")
    if city_name not in _cache["cities"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_city"], (new_id, city_name))
        _cache["cities"][city_name] = new_id
    return _cache["cities"][city_name]


def upsert_airport(session, stmts, airport_code: str, id_city: uuid.UUID) -> uuid.UUID:
    airport_code = safe_str(airport_code, "UNKNOWN")
    if airport_code not in _cache["airports"]:
        new_id = uuid.uuid4()
        session.execute(stmts["insert_airport"], (new_id, id_city, airport_code))
        _cache["airports"][airport_code] = new_id
    return _cache["airports"][airport_code]


# -------------------------------------------------------
# PROCESS FLIGHT
# -------------------------------------------------------

def process_flight(session, stmts, record: dict):
    """Inserisce un record di volo nelle tabelle normalizzate."""
    try:
        id_airline  = upsert_airline(session, stmts, record.get("AIRLINE"))
        id_aircraft = upsert_aircraft(session, stmts, record.get("TAIL_NUMBER"), id_airline)
        id_day      = upsert_day(session, stmts,
                                 record.get("YEAR"), record.get("MONTH"),
                                 record.get("DAY"), record.get("DAY_OF_WEEK"))

        origin_code = safe_str(record.get("ORIGIN_AIRPORT"), "UNKNOWN")
        dest_code   = safe_str(record.get("DESTINATION_AIRPORT"), "UNKNOWN")

        id_airport_origin = upsert_airport(session, stmts, origin_code,
                                           upsert_city(session, stmts, origin_code))
        id_airport_dest   = upsert_airport(session, stmts, dest_code,
                                           upsert_city(session, stmts, dest_code))

        id_state = uuid.uuid4()
        session.execute(stmts["insert_state"], (
            id_state,
            bool(record.get("CANCELLED", 0)),
            bool(record.get("DIVERTED", 0)),
            safe_str(record.get("CANCELLATION_REASON")),
        ))

        id_description = uuid.uuid4()
        session.execute(stmts["insert_description"], (
            id_description,
            safe_str(record.get("FLIGHT_NUMBER")),
            None, None,
            safe_int(record.get("SCHEDULED_TIME")),
        ))

        session.execute(stmts["insert_flight"], (
            uuid.uuid4(), id_day, id_description,
            id_airport_origin, id_airport_dest,
            id_aircraft, id_state,
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
        print(f"❌ Errore insert flight: {e}")


# -------------------------------------------------------
# PROCESS WEATHER
# -------------------------------------------------------

def process_weather(session, stmts, record: dict):
    """Inserisce un record meteo (formato long) in Cassandra."""
    try:
        dt = record.get("datetime")
        if isinstance(dt, str):
            dt = datetime.fromisoformat(dt)

        session.execute(stmts["insert_weather"], (
            uuid.uuid4(),
            safe_str(record.get("weather_description")),
            safe_float(record.get("wind_speed")),
            safe_str(record.get("wind_direction")),
            safe_str(record.get("city")),
            dt,
        ))

    except Exception as e:
        print(f"❌ Errore insert weather: {e}")


# -------------------------------------------------------
# WEATHER CACHE (per join con flight_weather)
# -------------------------------------------------------

def cache_weather(record: dict):
    """Salva il record meteo in cache per il join con i voli."""
    city = record.get("city", "")
    dt   = record.get("datetime")
    if not city or not dt:
        return
    date_str = str(dt)[:10]
    _weather_cache[(city, date_str)] = {
        "weather_type":   safe_str(record.get("weather_description")),
        "wind_speed":     record.get("wind_speed"),
        "wind_direction": safe_str(record.get("wind_direction")),
    }


# -------------------------------------------------------
# PROCESS FLIGHT_WEATHER (tabella denormalizzata)
# -------------------------------------------------------

def process_flight_weather(session, stmts, record: dict):
    """
    Inserisce nella tabella denormalizzata flight_weather
    unendo i dati del volo con il meteo della città di origine.
    """
    try:
        origin_iata = safe_str(record.get("ORIGIN_AIRPORT"), "")
        origin_city = IATA_TO_CITY.get(origin_iata)
        if not origin_city:
            return  # aeroporto non mappato, skip

        year  = record.get("YEAR")
        month = record.get("MONTH")
        day   = record.get("DAY")
        if not all([year, month, day]):
            return

        flight_date = date(int(year), int(month), int(day))
        date_str    = str(flight_date)

        weather = _weather_cache.get((origin_city, date_str), {})

        session.execute(stmts["insert_flight_weather"], (
            uuid.uuid4(),
            flight_date,
            origin_iata,
            origin_city,
            safe_int(record.get("DEPARTURE_DELAY")),
            safe_int(record.get("ARRIVAL_DELAY")),
            safe_float(record.get("DISTANCE")),
            weather.get("weather_type", "unknown"),
            safe_float(weather.get("wind_speed")),
            weather.get("wind_direction", ""),
            bool(record.get("CANCELLED", 0)),
        ))

    except Exception as e:
        print(f"❌ Errore insert flight_weather: {e}")
