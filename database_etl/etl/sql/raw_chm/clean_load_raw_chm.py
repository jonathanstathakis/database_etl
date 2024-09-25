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


def extracted_metadata_to_db(
    con: db.DuckDBPyConnection, lib_dir: Path, overwrite: bool = False
):
    logger.info("metadata_to_db..")
    if overwrite:
        con.execute("drop table if exists chm cascade")
        con.execute("drop sequence if exists chm_seq")
    paths = get_metadata_file_paths(lib_dir=lib_dir)

    # metadata_df is scanned in the query below
    metadata_df_ = pl.concat(
        [
            pl.read_parquet(path).with_columns(pl.lit(str(path.parent)).alias("path"))
            for path in paths
        ]
    )

    with open(get_load_raw_chm_query()) as f:
        query = f.read()

    pl.Config.set_fmt_str_lengths(999)

    # add a 'date' ordered numbering to runs with duplicate samplecodes.

    metadata_df = con.sql(
        """--sql
        with
            new_names as (
                select
                    date,
                    notebook,
                    rank_dense() over (
                        partition by notebook order by date) as rank,
                    concat(notebook, '_', rank) as new_name
                from
                    metadata_df_
                qualify
                    rank > 1
                        ),
            replaced_names as (
            select
                coalesce(new.new_name, orig.notebook) as notebook,
                orig.date as date,
                orig.method as method,
                orig.injection_volume as injection_volume,
                orig.seq_name as seq_name,
                orig.seq_desc as seq_desc,
                orig.vialnum as vialnum,
                orig.originalfilepath as originalfilepath,
                orig.id as id,
                orig."desc" as "desc",
                orig.path as path
            from
                metadata_df_ orig
            left join
                new_names new
            on
                orig.date = new.date
            and
                orig.notebook = new.notebook)
        select
            notebook,
            date,
            method,
            injection_volume,
            seq_name,
            seq_desc,
            vialnum,
            originalfilepath,
            id,
            "desc",
            path
        from
            replaced_names 
        """
    ).pl()

    # double check that samplecode is now unique

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

    con.execute(query)
