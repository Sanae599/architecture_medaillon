import pandas as pd
import numpy as np
import os
from faker import Faker

fake = Faker("fr_FR")

RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

NUM_NODES = 100_000
NUM_EDGES = 500_000

NODE_TYPES = ["Person", "Org", "Paper"]
REL_TYPES = ["works_at", "writes", "member_of", "cites", "collaborates_with"]

print(" Génération de données synthétiques réalistes")
print(f" {NUM_NODES} nœuds et {NUM_EDGES} arêtes seront créés\n")

#on génère une seule fois les types
node_labels = np.random.choice(NODE_TYPES, NUM_NODES)

#on crée les noms en fonction de ces types
node_names = [
    fake.name() if t == "Person"
    else fake.company() if t == "Org"
    else fake.catch_phrase()
    for t in node_labels
]

#on construit le DataFrame
nodes = pd.DataFrame({
    "id": np.arange(NUM_NODES),
    "label": node_labels,
    "name": node_names
})

nodes.to_csv(os.path.join(RAW_DIR, "nodes.csv"), index=False)
print(f" Fichier 'nodes.csv' créé ({len(nodes)} lignes)")

#génère les arêtes/relations
edges = pd.DataFrame({
    "src": np.random.randint(0, NUM_NODES, NUM_EDGES),
    "dst": np.random.randint(0, NUM_NODES, NUM_EDGES),
    "type": np.random.choice(REL_TYPES, NUM_EDGES)
})

edges.to_csv(os.path.join(RAW_DIR, "edges.csv"), index=False)
print(f"Fichier 'edges.csv' créé ({len(edges)} lignes)\n")
