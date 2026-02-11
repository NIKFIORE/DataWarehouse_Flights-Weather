# Progetto Data Warehouse – Analisi dell’Impatto delle Condizioni Meteo sui Voli

## Descrizione del Progetto

Questo progetto prevede la progettazione e realizzazione di un **Data Warehouse** per analizzare come le condizioni meteorologiche influenzino ritardi e cancellazioni dei voli negli Stati Uniti.

L’obiettivo è integrare e unificare più sorgenti di dati eterogenee, mettendo in relazione informazioni operative sui voli con dati meteorologici orari, al fine di studiare l’impatto delle variabili atmosferiche sulle performance del traffico aereo.

---

## Dataset Utilizzati

### 1. Dataset Voli (USA – 2015)

Dataset pubblico disponibile su Kaggle:  
👉 https://www.kaggle.com/datasets/usdot/flight-delays?select=flights.csv

Contiene informazioni sui voli negli Stati Uniti per l’anno 2015, tra cui:

- Informazioni temporali (anno, mese, giorno, giorno della settimana)
- Compagnia aerea e numero di volo
- Aeroporto di origine e destinazione
- Orari pianificati ed effettivi di partenza e arrivo
- Ritardi in partenza e arrivo
- Indicatori di cancellazione o diversione
- Cause del ritardo (meteo, compagnia, sistema aeroportuale, sicurezza, ecc.)

---

### 2. Dataset Meteorologici (2011–2017)

Dataset disponibile su Kaggle:  
👉 https://www.kaggle.com/datasets/selfishgene/historical-hourly-weather-data

Insieme di dataset meteo strutturati come serie temporali orarie, contenenti:

- Descrizione delle condizioni atmosferiche
- Velocità del vento
- Direzione del vento
- Altre variabili meteorologiche

Ogni dataset contiene una colonna `datetime` e una colonna per ciascuna città/aeroporto.  
Dopo il filtraggio sull’anno 2015, ciascun dataset meteo presenta circa **8.760 record** (uno per ogni ora dell’anno).

---

## Integrazione dei Dati

L’integrazione avviene su base:

- **Temporale** (data e ora del volo)
- **Geografica** (aeroporto/città)

Attraverso un processo ETL (Extract, Transform, Load), i dati vengono:

1. Puliti e normalizzati  
2. Allineati temporalmente  
3. Collegati tramite chiavi coerenti  
4. Caricati nel Data Warehouse per l’analisi multidimensionale  

---

## Obiettivi Analitici

Il Data Warehouse supporta analisi lungo le seguenti dimensioni:

- Tempo
- Aeroporto / Città
- Compagnia aerea
- Condizioni meteorologiche

### Esempi di domande di analisi

- In che misura la **velocità del vento** influisce sul ritardo medio dei voli?
- Esistono particolari **direzioni del vento** associate a un aumento delle cancellazioni?
- Come variano ritardi e cancellazioni al variare delle condizioni meteo?
- L’impatto del meteo è uniforme tra aeroporti o dipende dalla posizione geografica?
- Alcune compagnie risultano più resilienti a specifiche condizioni meteorologiche?

---

## Nota sui Dataset

I dataset originali non sono inclusi nella repository a causa delle dimensioni elevate.  
È possibile scaricarli direttamente dai link Kaggle sopra riportati.

---

## Finalità

Il progetto mira a dimostrare come l’integrazione di dataset eterogenei all’interno di un Data Warehouse consenta di evidenziare correlazioni significative tra fenomeni esterni (condizioni meteo) e performance operative (ritardi e cancellazioni), fornendo una base strutturata per analisi avanzate e supporto decisionale.
