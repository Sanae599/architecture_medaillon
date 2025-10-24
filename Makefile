seed:
	python3 scripts/generate_sample_data.py

bronze:
	python3 scripts/to_parquet.py

silver:
	python3 scripts/nodes_to_silver.py
	python3 scripts/partition_edges.py
	python3 quality/gx_checkpoint.py

gold:
	python3 scripts/convert_toneo4j_csv.py

import:
	bash scripts/import_goldtoneo4j_cypher_bash.sh

up:
	docker compose up -d

down:
	docker compose down -v

e2e: up seed bronze silver gold import

