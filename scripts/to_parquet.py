import os
import pandas as pd

raw_chemin = "data/raw"
bronze_chemin = "data/bronze"

os.makedirs(bronze_chemin, exist_ok=True)

for file in os.listdir(raw_chemin):
    if file.endswith(".csv"):
        csv_path = os.path.join(raw_chemin, file)
        parquet_path = os.path.join(bronze_chemin, file.replace(".csv", ".parquet"))

        print(f" Lecture : {csv_path}")
        df = pd.read_csv(csv_path)

        #sauvegarde au format Parquet (compression algo Snappy)
        df.to_parquet(parquet_path, index=False) #bibliotheqe pyarrrow de pandas 
        print(f"  Écrit : {parquet_path}")
