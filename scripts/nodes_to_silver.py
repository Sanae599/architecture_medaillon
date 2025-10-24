import os
import shutil
from pathlib import Path

bronze_chemin = Path("data/bronze")
silver_chemin = Path("data/silver")

src_file = bronze_chemin / "nodes.parquet"
dst_file = silver_chemin / "nodes.parquet"

#copier le fichier et le colle dans le dosier silver à l'identique
shutil.copy2(src_file, dst_file)
