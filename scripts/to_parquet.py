import os
import pandas as pd

RAW_DIR = "data/raw"
BRONZE_DIR = "data/bronze"

os.makedirs(BRONZE_DIR, exist_ok=True)

print(" Conversion des CSV vers Parquet")

for file in os.listdir(RAW_DIR):
    if file.endswith(".csv"):
        csv_path = os.path.join(RAW_DIR, file)
        parquet_path = os.path.join(BRONZE_DIR, file.replace(".csv", ".parquet"))

        print(f" Lecture : {csv_path}")
        df = pd.read_csv(csv_path)

        # Sauvegarde au format Parquet (compression algo Snappy)
        df.to_parquet(parquet_path, index=False) # Bibliotheqe pyarrrow de pandas 
        print(f"  Écrit : {parquet_path}")

print("\n Conversion ok. Les fichiers sont dans 'data/bronze/'.")
