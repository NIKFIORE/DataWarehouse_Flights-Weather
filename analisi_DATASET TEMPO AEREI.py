import pandas as pd

# Carica i dataset
weather_description = pd.read_csv('Dataset/weather_description.csv')
wind_direction = pd.read_csv('Dataset/wind_direction.csv')
wind_speed = pd.read_csv('Dataset/wind_speed.csv')
flights = pd.read_csv('Dataset/flights.csv')

#ELIMINAZIONE ELEMENTI ANNO DIVERSO DA 2015
# Converte la colonna datetime in formato datetime
weather_description['datetime'] = pd.to_datetime(weather_description['datetime'])
wind_direction['datetime'] = pd.to_datetime(wind_direction['datetime'])
wind_speed['datetime'] = pd.to_datetime(wind_speed['datetime'])

# Filtra solo i record dell'anno 2015
weather_description_2015 = weather_description[
    weather_description['datetime'].dt.year == 2015
]

wind_direction_2015 = wind_direction[
    wind_direction['datetime'].dt.year == 2015
]

wind_speed_2015 = wind_speed[
    wind_speed['datetime'].dt.year == 2015
]

# === weather_description ===
print("=== weather_description ===")
print(f"Numero record: {len(weather_description)}")
print("Colonne:")
print(weather_description.columns)
print("Primi 3 record:")
print(weather_description.head(3))
print("\n")

# === wind_direction ===
print("=== wind_direction ===")
print(f"Numero record: {len(wind_direction)}")
print("Colonne:")
print(wind_direction.columns)
print("Primi 3 record:")
print(wind_direction.head(3))
print("\n")

# === wind_speed ===
print("=== wind_speed ===")
print(f"Numero record: {len(wind_speed)}")
print("Colonne:")
print(wind_speed.columns)
print("Primi 3 record:")
print(wind_speed.head(3))

# === flights ===
print("=== flights ===")
print(f"Numero record: {len(flights)}")
print("Colonne:")
print(flights.columns)
print("Primi 3 record:")
print(flights.head(3))
