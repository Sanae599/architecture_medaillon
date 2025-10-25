import pandas as pd
import numpy as np
import os
from pathlib import Path

bronze_chemin = Path("data/bronze")
silver_chemin = Path("data/silver") / "edges"  

#nombre de shards
n_shards = 8

#charger les edges depuis le bronze
edges_path = bronze_chemin / "edges.parquet"
edges = pd.read_parquet(edges_path)

#attribution aléatoire d’un shard à chaque edge avec faker
edges["shard"] = np.random.randint(0, n_shards, len(edges))

#création des shards
for shard_id in range(n_shards):
    shard_dir = silver_chemin / f"shard={shard_id}"
    shard_dir.mkdir(parents=True, exist_ok=True)

    shard_df = edges[edges["shard"] == shard_id].drop(columns=["shard"])
    shard_path = shard_dir / "edges.parquet"
    shard_df.to_parquet(shard_path, index=False)
