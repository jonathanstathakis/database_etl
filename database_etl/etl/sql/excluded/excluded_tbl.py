import duckdb as db
import polars as pl
import logging

logger = logging.getLogger(__name__)


def find_time_outliers(con: db.DuckDBPyConnection, time_cutoff: float) -> pl.DataFrame:
    """
    return a pl df containing samples whose right side of time dim interval is less
    than `time_cutoff`
    """
    return con.execute(
        """--sql
    select
        *
    from
        image_stats
    join
        chm
    on
        image_stats.runid = chm.runid
    where
        mins_max < ?
    """,
        parameters=[time_cutoff],
    ).pl()


def create_excluded_tbl(con: db.DuckDBPyConnection) -> None:
    con.sql(
        """--sql
    create or replace table excluded (
        runid varchar primary key references chm(runid),
        reason varchar not null
    );
    """
    )


def exclude_sample(con: db.DuckDBPyConnection, runid: str, reason: str) -> None:
    """
    As shown in `find_time_outliers`, sample `runid` = 61 is an aborted run with a runtime
    of 14 seconds, and is to be added to the excluded list.
    """
    con.execute(
        """--sql
        insert into excluded
            select
                runid,
                ? as reason
            from
                chm
            where
                runid = ?;
        """,
        parameters=[reason, runid],
    )


def add_3_16_grads_to_excluded(con: db.DuckDBPyConnection) -> None:
    con.sql(
        """--sql
    insert into excluded
        select runid, 'gradient not equal to 2.5' as reason from gradients where gradient != 2.5
    """
    )


def create_inc_chm_view(con: db.DuckDBPyConnection) -> None:
    """
    creates a view consisting of the anti join of chm and excluded, resulting in the set
    of runs deemed includable in downstream analyses.
    """
    con.sql("""--sql
    create or replace view inc_chm as
        select
            *
        from
            chm
        anti join
            excluded
        on
            excluded.runid = chm.runid;
    """)


def create_inc_img_stats(con: db.DuckDBPyConnection) -> None:
    """
    masks `image_stats` by the difference from the `excluded` list, returning the runs
    which are included.
    """
    con.sql(
        """--sql
    create or replace view inc_img_stats as
        select
            *
        from
            image_stats ist
        anti join
            excluded exc
        on
            ist.runid = exc.runid;
    """
    )


def add_paths_table(con: db.DuckDBPyConnection) -> None:
    """
    add a paths lookup table from the 'included' samples in `inc_chm` with primary key runid with 3 columns: dir path, data path and metadata path
    """

    con.execute(
        """--sql
        create view run_data_paths as
            select
                runid as runid,
                path as path,
                concat(path, '/data.parquet') as data_path,
                concat(path, '/metadata.parquet') as meta_path
            from
                inc_chm
        """
    )


def gen_included_views(
    con: db.DuckDBPyConnection,
    excluded_samples: list[dict[str, str]] = [{}],
    overwrite: bool = False,
) -> None:
    """
    generate the `excluded` table, a collection of samples deemed inappropriate for
    inclusion in the main dataset, but possibly worth reclaiming. Creates the table,
    adds sample `runid` = 61, and creates two views: `inc_chm` and `inc_img_stats`
    reflecting the subset of the respective tables to the included samples.

    The resason for this approach is that downstream processes can add runs to the
    `excluded` table, and the views will be updated without manual intervention.
    """

    logger.info("gen_included_views..")
    if overwrite:
        con.execute("drop table if exists excluded")
        con.execute("drop view if exists inc_chm")
        con.execute("drop view if exists inc_img_stats")

    create_excluded_tbl(con=con)

    for sample in excluded_samples:
        if sample:
            exclude_sample(con=con, runid=sample["runid"], reason=sample["reason"])
    add_3_16_grads_to_excluded(con=con)
    create_inc_chm_view(con=con)
    create_inc_img_stats(con=con)
    add_paths_table(con=con)
