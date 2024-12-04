import duckdb as db
from pathlib import Path
import polars as pl
import logging

logger = logging.getLogger(__name__)


def get_metadata_file_paths(lib_dir: Path):
    if paths := list(lib_dir.glob("*/extract_*/metadata.parquet")):
        return paths
    else:
        raise ValueError(f"found no metadata files in {lib_dir}")


def get_load_raw_chm_query() -> Path:
    return Path(__file__).parent / "clean_load_raw_chm.sql"


def get_query(name: str):
    with open(Path(__file__).parent / "queries" / f"{name}.sql") as f:
        return f.read()


def load_chm(con: db.DuckDBPyConnection, lib_dir: Path, overwrite: bool = False):
    logger.info("metadata_to_db..")
    # if overwrite:
    # con.execute("drop table if exists chm cascade")
    # con.execute("drop sequence if exists chm_seq")
    paths = get_metadata_file_paths(lib_dir=lib_dir)

    # metadata_df is scanned in the query below
    metadata_df_ = pl.concat(
        [
            pl.read_parquet(path).with_columns(pl.lit(str(path.parent)).alias("path"))
            for path in paths
        ]
    )  # noqa: F841

    pl.Config.set_fmt_str_lengths(999)

    # add a 'date' ordered numbering to runs with duplicate runids.

    metadata_df = con.sql(get_query("make_runids_unique")).pl()  # noqa: F841

    # double check that runid is now unique

    assert (
        con.sql(
            """--sql
    select
        notebook
    from
        metadata_df
    group by
        notebook
    having
        count(*) > 1
    """
        )
        .pl()
        .is_empty()
    )

    con.execute(get_query("create_chm_loading"))
    con.execute(get_query("create_sequences"))
    con.execute(get_query("create_chm"))
