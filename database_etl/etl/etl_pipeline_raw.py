# need to exclude sample pk 61.

""" """

import numpy as np
import xarray as xr
from database_etl.etl import sql, to_xr
from database_etl.etl import ch_extractor
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


def get_sample_metadata(con: db.DuckDBPyConnection) -> pl.DataFrame:
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
    ).pl()


def get_paths(con: db.DuckDBPyConnection, runids: list[str]) -> pl.DataFrame:
    """
    get the run cs img file path from the previously created `inc_image_stats` table.
    """

    paths = con.execute(
        """--sql
    select
        runid,
        data_path
    from
        run_data_paths
    where
        runid in ?
    """,
        parameters=[
            runids,
        ],
    ).pl()

    return paths


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


def fetch_imgs(con: db.DuckDBPyConnection, runids: list[str]) -> list[pl.DataFrame]:
    """
    parse the parquet file of each run's chromatospectral image found in paths
    """

    # read the img file for each sample

    paths: pl.DataFrame = get_paths(con=con, runids=runids)

    imgs = []

    for runid, path in paths.partition_by("runid", as_dict=True).items():
        path = path["data_path"][0]
        img = pl.read_parquet(path)
        img = img.with_columns(
            pl.lit(runid[0]).alias("runid"), pl.lit(path).alias("path"), pl.all()
        )
        img = img.pipe(smooth_numeric_col, col='mins')
        imgs.append(img)
    return imgs


def clean_imgs(imgs: list[pl.DataFrame]) -> list[pl.DataFrame]:
    imgs = []

    for img in imgs:
        img = img.pipe(add_runid_to_img, con=con)
        img = img.with_columns(pl.lit(path).alias("path"))
        img = img.pipe(smooth_numeric_col, col="mins")

        imgs.append(img)

    return imgs


def get_metadata_as_dict(df: pd.DataFrame) -> dict:
    return df.set_index("runid").to_dict(orient="index")


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
    excluded_samples: list[dict[str, str]] = [{}],
    con: db.DuckDBPyConnection = db.connect(),
    run_extraction: bool = False,
    overwrite: bool = False,
) -> None:
    """
    transform a directory `data_dir` of chemstation .D dirs into a xarray dataset. A side effect is a persistant duckdb database of sample run metadata.

    :data_dir: a directory of chemstation .D dirs to be parsed and transformed.
    :dirty_st_path: input sample tracker table (parquet)
    :ct_un: cellartracker

    `run_extraction` decides whether to run the costly `extract_run_data` routine.
    """
    logger.info("etl_pipeline_raw..")

    # -- drop all tables in order of dependency
    # /* TODO: find a better method than this. The only alternatives I can see are
    # to delete the DB file, or allow updates on conflict, but id rather be explicit
    # than implicit..
    # */

    tables = [
        "solvents",
        "bin_pump_mech_params",
        "solvprop_over_mins",
        "excluded",
        "chm",
        "sequences",
        "chm_loading",
        "st",
        "ct",
    ]
    views = ["inc_chm", "run_data_paths"]
    objs = {"table": tables, "view": views}

    if overwrite:
        for key in objs:
            for obj in objs[key]:
                try:
                    logger.info(f"dropping {obj}")

                    con.execute(
                        f"""
                        drop {key} if exists {obj};
                        """
                    )
                except (db.CatalogException, db.CatalogException) as e:
                    e.add_note(f"error encountered when trying to drop: {obj}")
                    raise e

    if run_extraction:
        for path in data_dir.glob("*.D"):
            ch_extractor.extract_run_data(path, overwrite=run_extraction)

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
    logger.info(
        f"pipeline complete. Database created at {con.execute('select path from duckdb_databases').pl().item()}"
    )
    return None


def get_data_as_tuple(
    imgs: list[pl.DataFrame], metadata_df: pl.DataFrame
) -> list[tuple[pl.DataFrame, pl.DataFrame]]:
    data = []

    for img in imgs:
        runid = img["runid"].unique()
        if len(runid) > 1:
            raise ValueError("expecting 1 runid value in runid column")
        else:
            metadata = db.execute(
                """--sql
            select
                *
            from
                metadata_df
            where
                runid = ?
            """,
                parameters=[runid.item()],
            ).pl()
            # metadata = metadata_df.filter(pl.col("runid") == runid.item())

        data.append((img, metadata))

    return data


def get_data(
    output: str, con: db.DuckDBPyConnection, runids: list[str] = []
) -> xr.Dataset | None:
    """
    extract data in the library generated by 'etl_pipeline_raw' whose runid is given in
    `runids`. If no runids are provided, will return all samples in `inc_chm`.

    Files are found through the database in `con` which expected to perscribe to the structure
    created by `etl_pipeline_raw`
    """

    if con.sql("select count(*)>0 from inc_chm").pl().is_empty():
        raise RuntimeError("`inc_chm` is empty, may need to run `etl_pipeline_raw`")
    if not runids:
        result = con.sql(
            """--sql
        select
            runid
        from
            inc_chm
        """
        ).fetchall()

        if result:
            runids_ = [row[0] for row in result]
        else:
            RuntimeError("didnt receive expected output")
    else:
        runids_ = runids

    imgs: list[pl.DataFrame] = fetch_imgs(con=con, runids=runids_)

    metadata_df = get_sample_metadata(con=con)

    match output:
        case "xr":
            imgs_as_dict = get_imgs_as_dict_numeric_cols_only(imgs=imgs)
            metadata_as_dict = get_metadata_as_dict(df=metadata_df.to_pandas())
            return to_xr.data_dicts_to_xr(
                img_dict=imgs_as_dict, metadata_dict=metadata_as_dict
            )
        case "tuple":
            return get_data_as_tuple(imgs=imgs, metadata_df=metadata_df)
