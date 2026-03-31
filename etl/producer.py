# ===============================
# producer.py — Lettura CSV e invio a Kafka
# ===============================

import time
import pandas as pd
from kafka import KafkaProducer
import json

from etl.config import (
    KAFKA_BOOTSTRAP, FLIGHTS_TOPIC, WEATHER_TOPIC,
    FLIGHTS_CSV, WEATHER_DESC_CSV, WIND_DIR_CSV, WIND_SPEED_CSV,
    CHUNK_SIZE, STREAM_INTERVAL
)
from etl.transforms import melt_weather_chunk


def get_producer(max_retries: int = 10) -> KafkaProducer:
    """Connette il producer Kafka con retry automatico."""
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
    raise RuntimeError("❌ Impossibile connettersi a Kafka (producer)")


def stream_loop(producer: KafkaProducer):
    """
    Legge i CSV a chunk e li invia a Kafka in loop continuo.
    Simula un flusso dati continuativo.
    """
    print("🚀 Avvio streaming continuo (loop)...")
    loop = 0

    while True:
        loop += 1
        print(f"\n🔁 Inizio loop #{loop}")

        flights_reader      = pd.read_csv(FLIGHTS_CSV,      chunksize=CHUNK_SIZE)
        weather_desc_reader = pd.read_csv(WEATHER_DESC_CSV, chunksize=CHUNK_SIZE)
        wind_dir_reader     = pd.read_csv(WIND_DIR_CSV,     chunksize=CHUNK_SIZE)
        wind_speed_reader   = pd.read_csv(WIND_SPEED_CSV,   chunksize=CHUNK_SIZE)

        for f_chunk, wd_chunk, dir_chunk, ws_chunk in zip(
            flights_reader,
            weather_desc_reader,
            wind_dir_reader,
            wind_speed_reader,
        ):
            # Invia voli
            print(f"📦 [Loop #{loop}] Invio {len(f_chunk)} FLIGHT records")
            for record in f_chunk.to_dict(orient="records"):
                producer.send(FLIGHTS_TOPIC, record)

            # Trasforma e invia meteo
            weather_long = melt_weather_chunk(wd_chunk, dir_chunk, ws_chunk)
            print(f"🌤 [Loop #{loop}] Invio {len(weather_long)} WEATHER records")
            for record in weather_long.to_dict(orient="records"):
                producer.send(WEATHER_TOPIC, record)

            producer.flush()
            print(f"✅ Chunk inviato — prossimo tra {STREAM_INTERVAL}s\n")
            time.sleep(STREAM_INTERVAL)

        print(f"🔄 Loop #{loop} completato. Ricomincio...\n")
