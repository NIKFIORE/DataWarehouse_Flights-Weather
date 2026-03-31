# ===============================
# transforms.py — Trasformazioni dati
# ===============================

import pandas as pd


def melt_weather_chunk(desc_chunk: pd.DataFrame,
                       dir_chunk: pd.DataFrame,
                       speed_chunk: pd.DataFrame) -> pd.DataFrame:
    """
    Trasforma i 3 dataframe meteo dal formato wide al formato long.
    Output: datetime | city | weather_description | wind_direction | wind_speed
    """
    desc_long  = desc_chunk.melt(id_vars=["datetime"], var_name="city", value_name="weather_description")
    dir_long   = dir_chunk.melt(id_vars=["datetime"],  var_name="city", value_name="wind_direction")
    speed_long = speed_chunk.melt(id_vars=["datetime"], var_name="city", value_name="wind_speed")

    weather_long = desc_long.merge(dir_long,   on=["datetime", "city"], how="outer")
    weather_long = weather_long.merge(speed_long, on=["datetime", "city"], how="outer")

    weather_long.dropna(
        subset=["weather_description", "wind_direction", "wind_speed"],
        how="all",
        inplace=True
    )
    weather_long["datetime"] = pd.to_datetime(weather_long["datetime"])
    return weather_long


def safe_int(val):
    """Converte in int, restituisce None se NaN o non convertibile."""
    try:
        if pd.isna(val):
            return None
        return int(val)
    except (TypeError, ValueError):
        return None


def safe_float(val):
    """Converte in float, restituisce None se NaN o non convertibile."""
    try:
        if pd.isna(val):
            return None
        return float(val)
    except (TypeError, ValueError):
        return None


def safe_str(val, default: str = "") -> str:
    """Converte in stringa, restituisce default se None o NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return default
    return str(val)
