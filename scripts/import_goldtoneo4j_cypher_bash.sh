#!/bin/bash
set -ex
echo "Import des données dans le conteneur Neo4j existant"
docker exec neo4j_medaillon \
  neo4j-admin database import full \
  --overwrite-destination=true \
  --id-type=integer \
  --nodes=Entity=/import/nodes_neo4j.csv \
  --relationships=REL=/import/edges_neo4j.csv \
  -- \
  neo4j

