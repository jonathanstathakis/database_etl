# need to exclude sample pk 61.

"""
~0. extract ch~
~1. get_clean_ct~
~2. clean_load_raw_st~
~3. extracted_metadata_to_db~
~4. bin_pump_to_db~
~5. normalise_bin_pump_tbls~
~6. get_sample_gradients~
~7. load_image_stats~
~8. included list~
8. sql_to_xr
"""

import xarray as xr
from database_etl.etl import sql
from database_etl.etl import chm_extractor
from pathlib import Path
import duckdb as db

import logging

logger = logging.getLogger(__name__)


def etl_pipeline_raw(
    data_dir: Path,
    dirty_st_path: Path,
    ct_un: str,
    ct_pw: str,
    excluded_samples: list[dict[str, str]] = [{}],
    con: db.DuckDBPyConnection = db.connect(),
    run_extraction: bool = False,
    overwrite: bool = False,
) -> xr.Dataset:
    """
    transform a directory `data_dir` of chemstation .D dirs into a xarray dataset. A side effect is a persistant duckdb database of sample run metadata.

    :data_dir: a directory of chemstation .D dirs to be parsed and transformed.
    :dirty_st_path: input sample tracker table (parquet)
    :ct_un: cellartracker

    `run_extraction` decides whether to run the costly `extract_run_data` routine.
    """
    logger.info("etl_pipeline_raw..")

    if run_extraction:
        for path in data_dir.glob("*.D"):
            chm_extractor.extract_run_data(path, overwrite=run_extraction)

    sql.ct.get_clean_ct(pw=ct_pw, un=ct_un, con=con, output="db", overwrite=overwrite)

    sql.raw_st.clean_load_raw_st(con=con, dirty_st_path=dirty_st_path, overwrite=overwrite)

    sql.raw_chm.extracted_metadata_to_db(con=con, lib_dir=data_dir, overwrite=overwrite)

    sql.raw_chm.load_bin_pump_tbls(data_dir, con=con, overwrite=overwrite)

    sql.raw_chm.get_sample_gradients(con=con)  # TODO: need to add overwrite clause

    sql.cs.load_image_stats(lib_path=data_dir, con=con, overwrite=overwrite)

    sql.excluded.gen_included_views(
        con=con, excluded_samples=excluded_samples, overwrite=overwrite
    )

    return sql.to_xr.sql_to_xr(con=con)
