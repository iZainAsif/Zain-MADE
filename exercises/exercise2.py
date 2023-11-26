import pandas as pd
import sqlite3
import numpy as np

csv_url = "https://download-data.deutschebahn.com/static/datasets/haltestellen/D_Bahnhof_2020_alle.CSV"
df = pd.read_csv(csv_url, encoding="latin1", sep=";")

df = df.drop(columns=["Status"])
df = df.replace({'NAN': np.nan})
df = df.dropna()

df['EVA_NR'] = df['EVA_NR'].astype(int)
df['DS100'] = df['DS100'].astype('string')
df['IFOPT'] = df['IFOPT'].astype('string')
df['NAME'] = df['NAME'].astype('string')
df['Verkehr'] = df['Verkehr'].astype('string')
df['Betreiber_Name'] = df['Betreiber_Name'].astype('string')
df['Betreiber_Nr'] = df['Betreiber_Nr'].astype(float)
df.loc[:, 'Laenge'] = df['Laenge'].str.replace(',', '.').astype(float)
df.loc[:, 'Breite'] = df['Breite'].str.replace(',', '.').astype(float)

df = df[df["Verkehr"].isin(['FV','RV','nur DPN'])]

df = df[(df["Laenge"].between(-90,90)) & (df["Breite"].between(-90,90))]

def is_valid_ifopt(value):
    parts = value.split(":")
    if len(parts) == 3 and parts[0].isalpha() and len(parts[0]) == 2 and parts[1].isdigit() and all(part.isdigit() for part in parts[2].split(":")):
        return True
    return False

df = df[df["IFOPT"].apply(is_valid_ifopt)]

db_path = "trainstops.sqlite"
conn = sqlite3.connect(db_path)
df.to_sql(name="trainstops", con=conn, if_exists="replace", index=False)
conn.close()