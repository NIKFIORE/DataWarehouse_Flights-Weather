"""
dashboard.py — Streamlit dashboard per monitorare la pipeline ETL
Mostra in tempo reale i record inseriti in Cassandra.
"""

import streamlit as st
import pandas as pd
import time
from cassandra.cluster import Cluster

# ===============================
# CONNESSIONE
# ===============================

@st.cache_resource
def get_session():
    cluster = Cluster(["cassandra"])
    session = cluster.connect("flight_db")
    return session


# ===============================
# QUERY HELPERS
# ===============================

def count_table(session, table):
    try:
        row = session.execute(f"SELECT COUNT(*) FROM {table}").one()
        return row[0]
    except Exception:
        return 0


def sample_flights(session, limit=20):
    try:
        rows = session.execute(f"SELECT * FROM flight LIMIT {limit}")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def sample_weather(session, limit=20):
    try:
        rows = session.execute(f"SELECT * FROM weather LIMIT {limit}")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


def airline_counts(session):
    try:
        rows = session.execute("SELECT * FROM airline")
        return pd.DataFrame(rows)
    except Exception:
        return pd.DataFrame()


# ===============================
# LAYOUT
# ===============================

st.set_page_config(page_title="Flight DW Monitor", layout="wide")
st.title("✈️ Flight Data Warehouse — Monitor ETL")

session = get_session()

# Auto-refresh ogni 5 secondi
refresh = st.sidebar.slider("Auto-refresh (secondi)", 3, 30, 5)
placeholder = st.empty()

while True:
    with placeholder.container():

        # --- KPI ---
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("✈️ Flights", count_table(session, "flight"))
        col2.metric("🌤 Weather records", count_table(session, "weather"))
        col3.metric("🏢 Airlines", count_table(session, "airline"))
        col4.metric("🛫 Airports", count_table(session, "airport"))

        st.divider()

        # --- ULTIMI VOLI ---
        st.subheader("Ultimi voli inseriti")
        df_flights = sample_flights(session)
        if not df_flights.empty:
            st.dataframe(df_flights, use_container_width=True)
        else:
            st.info("Nessun volo ancora inserito.")

        st.divider()

        # --- ULTIMI WEATHER ---
        st.subheader("Ultimi record meteo (formato long)")
        df_weather = sample_weather(session)
        if not df_weather.empty:
            st.dataframe(df_weather, use_container_width=True)
        else:
            st.info("Nessun record meteo ancora inserito.")

        st.divider()

        # --- AIRLINES ---
        st.subheader("Compagnie aeree presenti")
        df_airlines = airline_counts(session)
        if not df_airlines.empty:
            st.dataframe(df_airlines, use_container_width=True)
        else:
            st.info("Nessuna compagnia aerea ancora inserita.")

        st.caption(f"Ultimo aggiornamento: {pd.Timestamp.now().strftime('%H:%M:%S')} — prossimo refresh tra {refresh}s")

    time.sleep(refresh)
