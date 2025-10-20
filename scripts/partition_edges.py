import pandas as pd
import numpy as np
import os

BRONZE_DIR = "data/bronze"
SILVER_DIR = "data/silver"

os.makedirs(SILVER_DIR, exist_ok=True)

NUM_SHARDS = 8

print(f" Partitionnement des edges en {NUM_SHARDS} shards")

edges = pd.read_parquet(os.path.join(BRONZE_DIR, "edges.parquet"))

# Attribution aléatoire d’un shard à chaque edge
edges["shard"] = np.random.randint(0, NUM_SHARDS, len(edges))

for shard_id in range(NUM_SHARDS):
    shard_dir = os.path.join(SILVER_DIR, f"shard={shard_id}")
    os.makedirs(shard_dir, exist_ok=True)

    shard_df = edges[edges["shard"] == shard_id].drop(columns=["shard"])
    shard_path = os.path.join(shard_dir, "edges.parquet")
    shard_df.to_parquet(shard_path, index=False)

    print(f" Shard {shard_id} : {len(shard_df)} lignes sauvegardées")

print("\nPartitionnement terminé. Les shards sont disponibles dans 'data/silver/'.")
