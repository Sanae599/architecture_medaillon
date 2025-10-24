import great_expectations as gx
import pandas as pd
import logging
from pathlib import Path
import pyarrow.dataset as ds

silver_dir = Path("data/silver")
nodes_file = silver_dir / "nodes.parquet"
edges_folder = silver_dir / "edges"

def load_edges(folder: Path) -> pd.DataFrame:
    dataset = ds.dataset(str(folder), format="parquet", partitioning="hive")
    return dataset.to_table().to_pandas()

print("Chargement des données...")
nodes_df = pd.read_parquet(nodes_file)
edges_df = load_edges(edges_folder)

print(f"Nodes : {nodes_df.shape[0]} lignes, {nodes_df.shape[1]} colonnes")
print(f"Edges : {edges_df.shape[0]} lignes, {edges_df.shape[1]} colonnes\n")

context = gx.get_context()
source = context.data_sources.add_pandas("silver_source")

nodes_asset = source.add_dataframe_asset(name="nodes")
edges_asset = source.add_dataframe_asset(name="edges")

nodes_batch_def = nodes_asset.add_batch_definition_whole_dataframe("nodes_batch")
edges_batch_def = edges_asset.add_batch_definition_whole_dataframe("edges_batch")

nodes_batch = nodes_batch_def.get_batch(batch_parameters={"dataframe": nodes_df})
edges_batch = edges_batch_def.get_batch(batch_parameters={"dataframe": edges_df})

expect_unique_id = gx.expectations.ExpectColumnValuesToBeUnique(column="id", severity="critical")
expect_not_null_id = gx.expectations.ExpectColumnValuesToNotBeNull(column="id", severity="critical")
expect_not_null_src = gx.expectations.ExpectColumnValuesToNotBeNull(column="src", severity="critical")
expect_not_null_dst = gx.expectations.ExpectColumnValuesToNotBeNull(column="dst", severity="critical")

print("Colonnes edges :", list(edges_df.columns))

print("\nValidation des nodes")
check_nodes_unique = nodes_batch.validate(expect_unique_id)
check_nodes_notnull = nodes_batch.validate(expect_not_null_id)

print("\nValidation des edges")
check_edges_src = edges_batch.validate(expect_not_null_src)
check_edges_dst = edges_batch.validate(expect_not_null_dst)


