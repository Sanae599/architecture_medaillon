import pandas as pd
import numpy as np
import os
from faker import Faker

# Initialisation de Faker (en français pour des noms réalistes)
fake = Faker("fr_FR")

# Crée le dossier de sortie
RAW_DIR = "data/raw"
os.makedirs(RAW_DIR, exist_ok=True)

# Taille du dataset
NUM_NODES = 1_000_00
NUM_EDGES = 5_000_00

# Types possibles de nœuds et de relations
NODE_TYPES = ["Person", "Org", "Paper"]
REL_TYPES = ["works_at", "writes", "member_of", "cites", "collaborates_with"]

print(" Génération de données synthétiques réalistes")
print(f" {NUM_NODES} nœuds et {NUM_EDGES} arêtes seront créés\n")

# Génère les nœuds
nodes = pd.DataFrame({
    "id": np.arange(NUM_NODES),
    "label": np.random.choice(NODE_TYPES, NUM_NODES),
    "name": [
        fake.name() if t == "Person"
        else fake.company() if t == "Org"
        else fake.catch_phrase()
        for t in np.random.choice(NODE_TYPES, NUM_NODES)
    ]
})

nodes.to_csv(os.path.join(RAW_DIR, "nodes.csv"), index=False)
print(f" Fichier 'nodes.csv' créé ({len(nodes)} lignes)")

# Génère les arêtes
edges = pd.DataFrame({
    "src": np.random.randint(0, NUM_NODES, NUM_EDGES),
    "dst": np.random.randint(0, NUM_NODES, NUM_EDGES),
    "type": np.random.choice(REL_TYPES, NUM_EDGES)
})

edges.to_csv(os.path.join(RAW_DIR, "edges.csv"), index=False)
print(f"Fichier 'edges.csv' créé ({len(edges)} lignes)\n")

print(" Génération terminée avec succès. Les fichiers sont disponibles dans 'data/raw/'.")
