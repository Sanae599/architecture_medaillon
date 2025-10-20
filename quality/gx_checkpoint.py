import pandas as pd
import os
import sys

BRONZE_DIR = "data/bronze"

nodes_path = os.path.join(BRONZE_DIR, "nodes.parquet")
edges_path = os.path.join(BRONZE_DIR, "edges.parquet")

print(" Vérification de la qualité des données")

# Chargement des fichiers
nodes = pd.read_parquet(nodes_path)
edges = pd.read_parquet(edges_path)

# Vérifie l'unicité des IDs dans nodes
if nodes["id"].is_unique:
    print(" Les IDs des nœuds sont uniques.")
else:
    print(" Doublons détectés dans les IDs des nœuds")
    duplicates = nodes[nodes["id"].duplicated()]["id"].tolist()[:10]
    print(f"   Exemples d'IDs dupliqués : {duplicates}")
    sys.exit(1)

# Vérifie l'absence de valeurs nulles dans src et dst
if edges["src"].isnull().any() or edges["dst"].isnull().any():
    print(" Des valeurs nulles détectées dans 'src' ou 'dst'")
    sys.exit(1)
else:
    print(" Aucune valeur nulle dans les colonnes 'src' et 'dst'")

print("\n Toutes les vérifications sont passées avec succès ")
