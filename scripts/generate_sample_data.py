import pandas as pd
import numpy as np
import os
from faker import Faker

fake = Faker("fr_FR")

raw_chemin = "data/raw"
os.makedirs(raw_chemin, exist_ok=True)

#taille des variables et creation
n_nodes = 100_000
n_edges = 500_000
node_types = ["person", "org", "paper"]
rel_types = ["works_at", "writes", "member_of", "cites", "collaborates_with"]

#on génère une seule fois les types
node_labels = np.random.choice(node_types, n_nodes)

#on crée les noms en fonction de ces types
node_names = [
    fake.name() if t == "person"
    else fake.company() if t == "org"
    else fake.catch_phrase()
    for t in node_labels
]

#construction du DataFrame
nodes = pd.DataFrame({
    "id": np.arange(n_nodes),
    "label": node_labels,
    "name": node_names
})

nodes.to_csv(os.path.join(raw_chemin, "nodes.csv"), index=False)

#génération des arêtes/relations
edges = pd.DataFrame({
    "src": np.random.randint(0, n_nodes, n_edges),
    "dst": np.random.randint(0, n_nodes, n_edges),
    "type": np.random.choice(rel_types, n_edges)
})

edges.to_csv(os.path.join(raw_chemin, "edges.csv"), index=False)
