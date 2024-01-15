import pandas as pd
import zipfile
import os
import urllib.request
import sqlite3

source = "https://www.mowesta.com/data/measure/mowesta-dataset-20221107.zip"
file = "mowesta-dataset.zip"
folder = "mowesta-dataset"

urllib.request.urlretrieve(source, file)

with zipfile.ZipFile(file, 'r') as zip_ref:
    zip_ref.extractall(folder)

data = os.path.join(folder, "data.csv")


df = pd.read_csv(data,sep=";",decimal=",",index_col=False,
                 usecols=["Geraet", "Hersteller", "Model",
                          "Monat", "Temperatur in °C (DWD)",
                          "Batterietemperatur in °C", "Geraet aktiv"])

df.rename(columns={"Temperatur in °C (DWD)": "Temperatur",
                   "Batterietemperatur in °C": "Batterietemperatur"}, inplace=True)

df["Temperatur"] = (df["Temperatur"] * 9/5) + 32
df["Batterietemperatur"] = (df["Batterietemperatur"] * 9/5) + 32

df = df[df["Geraet"] > 0]
df = df[(df["Monat"] >= 1) & (df["Monat"] <= 12)]
df = df[(df["Geraet aktiv"] == 'Ja') | (df["Geraet aktiv"] == 'Nein')]

db_path = "temperatures.sqlite"
table = "temperatures"
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

query= f"""
    CREATE TABLE IF NOT EXISTS temperatures (
    Geraet BIGINT,
    Hersteller TEXT,
    Model TEXT,
    Monat TEXT,
    Temperatur FLOAT,
    Batterietemperatur FLOAT,
    Geraet_aktiv TEXT)"""
cursor.execute(query)

df.to_sql(table, conn, if_exists='replace', index=False)
conn.commit()
conn.close()