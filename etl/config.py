# ===============================
# config.py — Parametri globali
# ===============================

# Kafka
KAFKA_BOOTSTRAP = "kafka:9092"
FLIGHTS_TOPIC   = "flights_topic"
WEATHER_TOPIC   = "weather_topic"

# Cassandra
CASSANDRA_HOST = "cassandra"
KEYSPACE       = "flight_db"

# Streaming
CHUNK_SIZE      = 5000
STREAM_INTERVAL = 10  # secondi tra un chunk e il successivo

# Dataset
DATASET_DIR      = "/app/Dataset"
FLIGHTS_CSV      = f"{DATASET_DIR}/flights.csv"
WEATHER_DESC_CSV = f"{DATASET_DIR}/weather_description.csv"
WIND_DIR_CSV     = f"{DATASET_DIR}/wind_direction.csv"
WIND_SPEED_CSV   = f"{DATASET_DIR}/wind_speed.csv"

# Kaggle dataset slugs
KAGGLE_FLIGHTS_DATASET = "usdot/flight-delays"
KAGGLE_WEATHER_DATASET = "selfishgene/historical-hourly-weather-data"

# Mapping giorno della settimana
DAY_OF_WEEK_MAP = {
    1: "Monday",
    2: "Tuesday",
    3: "Wednesday",
    4: "Thursday",
    5: "Friday",
    6: "Saturday",
    7: "Sunday",
}

# Mappatura codice IATA -> nome città (dataset meteo)
IATA_TO_CITY = {
    "SEA": "Seattle",
    "PDX": "Portland",
    "SFO": "San Francisco",
    "LAX": "Los Angeles",
    "SAN": "San Diego",
    "LAS": "Las Vegas",
    "PHX": "Phoenix",
    "ABQ": "Albuquerque",
    "DEN": "Denver",
    "SAT": "San Antonio",
    "DAL": "Dallas",
    "HOU": "Houston",
    "MCI": "Kansas City",
    "MSP": "Minneapolis",
    "STL": "Saint Louis",
    "ORD": "Chicago",
    "BNA": "Nashville",
    "IND": "Indianapolis",
    "ATL": "Atlanta",
    "DTW": "Detroit",
    "JAX": "Jacksonville",
    "CLT": "Charlotte",
    "MIA": "Miami",
    "PIT": "Pittsburgh",
    "PHL": "Philadelphia",
    "JFK": "New York",
    "BOS": "Boston",
    "YVR": "Vancouver",
    "YYZ": "Toronto",
    "YUL": "Montreal",
}
