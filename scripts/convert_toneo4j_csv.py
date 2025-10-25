import pandas as pd
import os
from pathlib import Path

silver_chemin = Path("data/silver")
edges_chemin = silver_chemin / "edges"
gold_chemin = Path("data/gold")

os.makedirs(gold_chemin, exist_ok=True)

#lecture du fichier nodes.parquet
nodes = pd.read_parquet(silver_chemin / "nodes.parquet")

#lecture de tous les fichiers edges.parquet dans les shards
edges_list = []
for shard_dir in edges_chemin.glob("shard=*"):
    edges_path = shard_dir / "edges.parquet"
    if edges_path.exists():
        edges_list.append(pd.read_parquet(edges_path))

edges = pd.concat(edges_list, ignore_index=True)

#renomme mes colonnes pour Neo4j
nodes = nodes.rename(columns={
    "id": "id:ID",
    "name": "name",
    "label": "label"
})

edges = edges.rename(columns={
    "src": ":START_ID",
    "dst": ":END_ID",
    "type": "type"
})

nodes.to_csv(gold_chemin / "nodes_neo4j.csv", index=False)
edges.to_csv(gold_chemin / "edges_neo4j.csv", index=False)
