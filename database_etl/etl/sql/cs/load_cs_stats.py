import polars as pl
import duckdb as db
from pathlib import Path
import logging

logger = logging.getLogger(__name__)


def get_image_filepaths(lib_path: Path) -> list[Path]:
    return list(lib_path.glob("*.D/extract_*/data.parquet"))


def image_stats_from_files(image_files: list[Path]) -> pl.DataFrame:
    image_stats_ = []

    for f in list(image_files):
        df = pl.read_parquet(f)

        # nm
        nm_min = df.columns[2]
        nm_max = df.columns[-1]
        nm_count = len(df.columns)

        # time
        mins_min = df.select("mins").min().item()
        mins_max = df.select("mins").max().item()
        mins_count = df.select("mins").count().item()

        # abs @ 256
        abs_min = df.select("256").min().item()
        abs_max = df.select("256").max().item()
        abs_argmin = df.select("256").to_series().arg_min()
        abs_argmax = df.select("256").to_series().arg_max()

        # hertz
        hertz = df.select(pl.col("mins").diff().mul(60).pow(-1).mean().round(2))

        image_stats_.append(
            pl.DataFrame(
                {
                    "id": df.select("id")[0].item(),
                    "nm_min": nm_min,
                    "nm_max": nm_max,
                    "nm_count": nm_count,
                    "mins_min": mins_min,
                    "mins_max": mins_max,
                    "mins_count": mins_count,
                    "abs_min": abs_min,
                    "abs_max": abs_max,
                    "abs_argmin": abs_argmin,
                    "abs_argmax": abs_argmax,
                    "hertz": hertz,
                    "path": str(f),
                }
            )
        )
    image_stats_df = pl.concat(image_stats_)

    return image_stats_df


def load_image_stats_to_db(con: db.DuckDBPyConnection, image_stats_df: pl.DataFrame):
    """
    Expects chm to be in db
    """
    image_stats_df = image_stats_df  # to fool lsp
    con.sql(
        """--sql
    create or replace table image_stats (
        runid varchar primary key,
        nm_min integer not null,
        nm_max integer not null,
        nm_count integer not null,
        mins_min float not null,
        mins_max float not null,
        mins_count integer not null,
        abs_min float not null,
        abs_max float not null,
        abs_argmin float not null,
        abs_argmax float not null,
        hertz float not null,
        path varchar not null unique,
    );
    insert into image_stats
        select
            chm.runid,
            img.nm_min,
            img.nm_max,
            img.nm_count,
            img.mins_min,
            img.mins_max,
            img.mins_count as mins_count,
            img.abs_min,
            img.abs_max,
            img.abs_argmin,
            img.abs_argmax,
            img.hertz,
            img.path
        from
            image_stats_df img
        join
            chm
        using
            (id)
        order by
            chm.runid;
    """
    )


def load_image_stats(
    lib_path: Path, con: db.DuckDBPyConnection, overwrite: bool = False
) -> None:
    """
    expects chm to be in db
    """
    logger.info("load_image_stats..")
    if overwrite:
        con.execute("drop table if exists image_stats")

    if (
        "chm"
        not in con.execute("select name from (show tables) where name ='chm'").pl()[
            "name"
        ]
    ):
        raise RuntimeError("Expect chm to be in db")

    image_file_paths = get_image_filepaths(lib_path=lib_path)
    image_stats_df = image_stats_from_files(image_files=image_file_paths)
    load_image_stats_to_db(con=con, image_stats_df=image_stats_df)
