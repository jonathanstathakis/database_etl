import duckdb as db

import logging

logger = logging.getLogger(__name__)


def calculate_gradient(con: db.DuckDBPyConnection) -> None:
    if not con.sql(
        """--sql
    select
        *
    from
        solvprop_over_time
    """
    ).fetchall():
        raise ValueError("solvprop_over_time is empty")
    con.sql(
        """--sql
    CREATE OR REPLACE view gradients AS (
        select
            samplecode,
            percent_diff/time as gradient
        from (
            select
                samplecode,
                idx,
                time,
                percent,
                percent - lag(percent) OVER (PARTITION BY samplecode ORDER BY idx) as percent_diff,
                --lag(percent) as percent_shift,
            from  (
                select
                samplecode,
                dense_rank() OVER (partition by samplecode order by time) as idx,
                time,
                percent,
                from
                    solvprop_over_time
                where
                    channel = 'b'
            )
        )
        where
            idx = 2
        ORDER BY
            samplecode,
            time
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
