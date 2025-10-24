import pandas as pd
import numpy as np
import os
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver") / "edges"  

NUM_SHARDS = 8

print(f" Partitionnement des edges en {NUM_SHARDS} shards")

#charger les edges depuis le bronze
edges_path = BRONZE_DIR / "edges.parquet"
edges = pd.read_parquet(edges_path)

#attribution aléatoire d’un shard à chaque edge avec faker
edges["shard"] = np.random.randint(0, NUM_SHARDS, len(edges))

#création des shards
for shard_id in range(NUM_SHARDS):
    shard_dir = SILVER_DIR / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    shard_df = edges[edges["shard"] == shard_id].drop(columns=["shard"])
    shard_path = shard_dir / "edges.parquet"
    shard_df.to_parquet(shard_path, index=False)

    print(f" Shard {shard_id} : {len(shard_df)} lignes bien sauvegardées")
