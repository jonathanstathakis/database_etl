import duckdb as db
from pathlib import Path
from .bin_pump_to_db_ import bin_pump_to_db
from .normalise_bin_pump_tbls import normalise_bin_pump_tbls
import logging

logger = logging.getLogger(__name__)


def load_bin_pump_tbls(
    data_dir: Path, con: db.DuckDBPyConnection, overwrite: bool = True
) -> None:
    """
    execute both `bin_pump_to_db` and `normalise_bin_pump_tbls`
    """

    logger.info("load_bin_pump_tbls..")

    result = list(data_dir.glob("*.D"))
    if not result:
        raise ValueError("didnt find any paths matching pattern '*.D'")

    bin_pump_to_db(paths=result, con=con, overwrite=overwrite)
    normalise_bin_pump_tbls(con=con, overwrite=overwrite)
