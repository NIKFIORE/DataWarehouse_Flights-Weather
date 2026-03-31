# ===============================
# kaggle_downloader.py — Download dataset da Kaggle API
# ===============================

import os
import json
import subprocess
from etl.config import (
    DATASET_DIR,
    FLIGHTS_CSV, WEATHER_DESC_CSV, WIND_DIR_CSV, WIND_SPEED_CSV,
    KAGGLE_FLIGHTS_DATASET, KAGGLE_WEATHER_DATASET
)


def _setup_credentials():
    """Scrive kaggle.json dalle variabili d'ambiente."""
    token = os.environ.get("KAGGLE_API_TOKEN")
    if not token:
        raise RuntimeError(
            "❌ KAGGLE_API_TOKEN non impostato! "
            "Aggiungilo nel docker-compose.yml sotto environment."
        )

    kaggle_dir = os.path.expanduser("~/.config/kaggle")
    os.makedirs(kaggle_dir, exist_ok=True)
    cred_path = os.path.join(kaggle_dir, "kaggle.json")

    # Supporta sia il nuovo formato token (KGAT_...) che il vecchio username:key
    if token.startswith("KGAT_"):
        creds = {"token": token}
    elif ":" in token:
        username, key = token.split(":", 1)
        creds = {"username": username, "key": key}
    else:
        raise RuntimeError("❌ Formato KAGGLE_API_TOKEN non riconosciuto.")

    with open(cred_path, "w") as f:
        json.dump(creds, f)
    os.chmod(cred_path, 0o600)
    print("✅ Credenziali Kaggle configurate")


def _download(dataset_slug: str, dest_dir: str):
    """Scarica e decomprime un dataset Kaggle nella cartella dest_dir."""
    print(f"📥 Download: {dataset_slug} → {dest_dir}")
    subprocess.run(
        ["kaggle", "datasets", "download", "-d", dataset_slug, "-p", dest_dir, "--unzip"],
        check=True
    )
    print(f"✅ {dataset_slug} scaricato")


def ensure_datasets():
    """
    Controlla se i CSV necessari sono già presenti.
    Se mancano, li scarica da Kaggle.
    Viene chiamato all'avvio dell'ETL.
    """
    os.makedirs(DATASET_DIR, exist_ok=True)

    needed = [FLIGHTS_CSV, WEATHER_DESC_CSV, WIND_DIR_CSV, WIND_SPEED_CSV]
    missing = [f for f in needed if not os.path.exists(f)]

    if not missing:
        print("✅ Dataset già presenti, skip download Kaggle.")
        return

    print(f"⚠️  File mancanti: {[os.path.basename(f) for f in missing]}")
    _setup_credentials()

    if not os.path.exists(FLIGHTS_CSV):
        _download(KAGGLE_FLIGHTS_DATASET, DATASET_DIR)

    weather_files = [WEATHER_DESC_CSV, WIND_DIR_CSV, WIND_SPEED_CSV]
    if any(not os.path.exists(f) for f in weather_files):
        _download(KAGGLE_WEATHER_DATASET, DATASET_DIR)

    # Verifica finale
    still_missing = [f for f in needed if not os.path.exists(f)]
    if still_missing:
        raise RuntimeError(
            f"❌ Alcuni file non trovati dopo il download: "
            f"{[os.path.basename(f) for f in still_missing]}\n"
            f"Controlla che i CSV siano nel path corretto dentro il dataset Kaggle."
        )

    print("🎯 Tutti i dataset pronti.")
