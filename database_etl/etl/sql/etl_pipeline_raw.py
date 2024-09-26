# need to exclude sample pk 61.

""" """

import numpy as np
import xarray as xr
from database_etl.etl import sql, to_xr
from database_etl.etl import chm_extractor
from pathlib import Path
import duckdb as db
import pandas as pd
import polars as pl
import logging

logger = logging.getLogger(__name__)


def smooth_numeric_col(df: pl.DataFrame, col: str) -> pl.DataFrame:
    """
    relabel the time index of the df to the observation frequency evenly spaced, from
    0 to the last value of the index. Assumes that the index is the time dimension,
    in minute units and that the last value of the index is the maximum.
    """

    df = df.with_columns(
        pl.lit(smooth_numeric_array(df[col].to_numpy(writable=True))).alias(col)
    )

    return df


def smooth_numeric_array(input_time):
    if not isinstance(input_time, np.ndarray):
        raise TypeError("expect numpy array")

    input_time.sort()
    time_max = input_time.max()
    mean_timestep = np.round(np.mean(np.diff(input_time)), 9)

    # the slicing is due to a difficult to fix discrepency in samples with 1 more observation than the rest. Just makes sure tht the lengths of the indexes is the same. It may cause errors downstream..

    return np.arange(0, time_max + mean_timestep, mean_timestep)[0 : len(input_time)]


def get_sample_metadata(con: db.DuckDBPyConnection) -> pd.DataFrame:
    """
    return each run's metadata as determined by its unique id.
    """
    return con.sql(
        """--sql
    select
        chm.id as id,
        chm.runid as runid,
        chm.acq_date as acq_date,
        chm.acq_method as acq_method,
        chm.seq_name as seq_name,
        chm.description as description,
        st.detection as detection,
        ct.vintage::integer as vintage,
        ct.wine as wine,
        ct.locale as locale,
        ct.country as country,
        ct.region as region,
        ct.subregion as subregion,
        ct.appellation as appellation,
        ct.producer as producer,
        ct.type as type,
        ct.color as color,
        ct.category as category,
        ct.varietal as varietal
    from    
        chm
    left join
        st
    on
        chm.samplecode = st.samplecode
    left join
        ct
    on
        ct.vintage = st.vintage
    and
        st.wine = ct.wine
    anti join
        excluded exc
    on
        chm.runid = exc.runid
    """
    ).df()


def get_paths(con: db.DuckDBPyConnection) -> list[str]:
    """
    get the run cs img file path from the previously created `inc_image_stats` table.
    """

    paths = con.sql("""--sql
    select
        data_path
    from
        run_data_paths
    """).fetchall()

    return [path[0] for path in paths]


def add_runid_to_img(img: pl.DataFrame, con: db.DuckDBPyConnection) -> pl.DataFrame:
    """
    join the img file to chm to add the runid
    """

    return con.sql(
        """--sql
    select
        chm.runid,
        img.*
    from
        img
    left join
        chm
    on
        chm.id = img.id
    """
    ).pl()


def fetch_imgs(con: db.DuckDBPyConnection) -> list[pl.DataFrame]:
    """
    parse the parquet file of each run's chromatospectral image found in paths
    """

    # read the img file for each sample
    paths = get_paths(con=con)

    # join path to data here

    imgs = []

    for path in paths:
        img = pl.read_parquet(path)
        img = img.pipe(add_runid_to_img, con=con)
        img = img.with_columns(pl.lit(path).alias("path"))
        img = img.rename({"time": "mins"})
        img = img.pipe(smooth_numeric_col, col="mins")

        imgs.append(img)

    return imgs


def get_metadata_as_dict(con: db.DuckDBPyConnection) -> dict:
    metadata = get_sample_metadata(con=con)

    return metadata.set_index("runid").to_dict(orient="index")


def get_imgs_as_dict_numeric_cols_only(
    imgs: list[pl.DataFrame],
) -> dict[str, pl.DataFrame]:
    """
    fetch all chromatospectral images for all samples in included chm returned as a
    dict with the 'chm.id' as the keys and the image dataframe as the value.
    """

    return {img["runid"][0]: img.drop(["id", "runid", "path"]) for img in imgs}


def etl_pipeline_raw(
    data_dir: Path,
    dirty_st_path: Path,
    ct_un: str,
    ct_pw: str,
    output: str,
    excluded_samples: list[dict[str, str]] = [{}],
    con: db.DuckDBPyConnection = db.connect(),
    run_extraction: bool = False,
    overwrite: bool = False,
) -> xr.Dataset | None:
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

    sql.ct.load_ct(pw=ct_pw, un=ct_un, con=con, output="db", overwrite=overwrite)

    sql.raw_st.clean_load_raw_st(
        con=con, dirty_st_path=dirty_st_path, overwrite=overwrite
    )

    sql.raw_chm.load_chm(con=con, lib_dir=data_dir, overwrite=overwrite)

    sql.raw_chm.load_bin_pump_tbls(data_dir, con=con, overwrite=overwrite)

    sql.raw_chm.get_sample_gradients(con=con)  # TODO: need to add overwrite clause

    sql.cs.load_image_stats(lib_path=data_dir, con=con, overwrite=overwrite)

    sql.excluded.gen_included_views(
        con=con, excluded_samples=excluded_samples, overwrite=overwrite
    )

    match output:
        case "xr":
            imgs = fetch_imgs(con=con)
            imgs_as_dict = get_imgs_as_dict_numeric_cols_only(imgs=imgs)
            metadata_as_dict = get_metadata_as_dict(con=con)
            return to_xr.data_dicts_to_xr(
                img_dict=imgs_as_dict, metadata_dict=metadata_as_dict
            )
        case None:
            # no output, but the database and extracted files may be kept persistently..
            return None
