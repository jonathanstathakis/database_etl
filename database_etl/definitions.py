import os
from pathlib import Path

ROOT_DIR = Path(os.path.dirname(os.path.abspath(__file__)))

BAD_CUPRAC_SAMPLES = ["128", "161", "163", "164", "165", "ca0101", "ca0301"]

DB_PATH: str = str(Path(ROOT_DIR) / "wine.db")
DATA_DIR = ROOT_DIR / "data"
RAW_DATA_LIB: Path = DATA_DIR / "raw_uv"
NC_DSET_PATH = DATA_DIR / "raw_uv.nc"
DIRTY_ST_PATH = (
    "/Users/jonathan/mres_thesis/data/dirty_sample_tracker_names_corrected.parquet"
)
CT_UN = os.environ["CELLAR_TRACKER_UN"]
CT_PW = os.environ["CELLAR_TRACKER_PW"]

EXCLUDED_RAW_SAMPLES = [
    {
        "runid": "2021-debortoli-cabernet-merlot_avantor",
        "reason": "aborted run",
    },
    {
        "runid": "98",
        "reason": "recorded at 5Hz. Possible to downsample but not worth it atm.",
    },
]
