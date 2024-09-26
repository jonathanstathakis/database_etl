import duckdb as db

import logging

logger = logging.getLogger(__name__)


def calculate_gradient(con: db.DuckDBPyConnection) -> None:
    if not con.sql(
        """--sql
    select
        *
    from
        solvprop_over_mins
    """
    ).fetchall():
        raise ValueError("solvprop_over_mins is empty")
    con.sql(
        """--sql
    CREATE OR REPLACE view gradients AS (
        select
            runid,
            percent_diff/mins as gradient
        from (
            select
                runid,
                idx,
                mins,
                percent,
                percent - lag(percent) OVER (PARTITION BY runid ORDER BY idx) as percent_diff,
                --lag(percent) as percent_shift,
            from  (
                select
                runid,
                dense_rank() OVER (partition by runid order by mins) as idx,
                mins,
                percent,
                from
                    solvprop_over_mins
                where
                    channel = 'b'
            )
        )
        where
            idx = 2
        ORDER BY
            runid,
            mins
    );
    select
        *
    from
        gradients
    limit 5 
    """
    )


def counts_per_gradient(con: db.DuckDBPyConnection) -> None:
    con.sql(
        """--sql
        create or replace view counts_per_gradient as (
            select
                gradient,
                count(*) as count
            from
                gradients
            group by gradient
            order by
                gradient
            )
        """
    )


def get_sample_gradients(con: db.DuckDBPyConnection) -> None:
    logger.info("get_sample_gradients..")
    calculate_gradient(con=con)
    counts_per_gradient(con=con)
