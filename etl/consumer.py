# ===============================
# consumer.py — Ascolto Kafka e inserimento in Cassandra
# ===============================

import time
import json
from kafka import KafkaConsumer

from etl.config import KAFKA_BOOTSTRAP, FLIGHTS_TOPIC, WEATHER_TOPIC
from etl.inserters import process_flight, process_weather, cache_weather, process_flight_weather


def get_consumer(max_retries: int = 10) -> KafkaConsumer:
    """Connette il consumer Kafka con retry automatico."""
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                FLIGHTS_TOPIC,
                WEATHER_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_deserializer=lambda x: json.loads(x.decode("utf-8")),
                auto_offset_reset="earliest",
                group_id="etl-group",
            )
            print("✅ Consumer Kafka connesso")
            return consumer
        except Exception as e:
            print(f"⏳ Kafka non pronto (tentativo {attempt+1}/{max_retries}): {e}")
            time.sleep(6)
    raise RuntimeError("❌ Impossibile connettersi a Kafka (consumer)")


def consume_loop(session, stmts):
    """
    Ascolta i topic Kafka e inserisce i messaggi in Cassandra.
    - weather → inserisce in weather E aggiorna la cache per il join
    - flights → inserisce in flight E in flight_weather (con meteo dalla cache)
    """
    consumer = get_consumer()
    print("👂 Consumer in ascolto...\n")

    for message in consumer:
        record = message.value

        if message.topic == WEATHER_TOPIC:
            process_weather(session, stmts, record)
            cache_weather(record)

        elif message.topic == FLIGHTS_TOPIC:
            process_flight(session, stmts, record)
            process_flight_weather(session, stmts, record)
