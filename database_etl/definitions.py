import os
from pathlib import Path

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

BAD_CUPRAC_SAMPLES = ["128", "161", "163", "164", "165", "ca0101", "ca0301"]

DB_PATH: str = str(Path(ROOT_DIR) / "wine.db")
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_LIB: Path = DATA_DIR / "raw_uv"
NC_DSET_PATH = DATA_DIR / "raw_uv.nc"
