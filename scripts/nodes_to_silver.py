import os
import shutil
from pathlib import Path

BRONZE_DIR = Path("data/bronze")
SILVER_DIR = Path("data/silver")

src_file = BRONZE_DIR / "nodes.parquet"
dst_file = SILVER_DIR / "nodes.parquet"

#copier le fichier et colle fichier dans le dosier silver
shutil.copy2(src_file, dst_file)
