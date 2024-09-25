import duckdb as db


def create_pump_mech_params(
    con: db.DuckDBPyConnection, overwrite: bool = False
) -> None:
    if overwrite:
        con.execute(
            """--sql
            drop table if exists bin_pump_mech_params;
            """
        )
    con.execute(
        """--sql
    create table bin_pump_mech_params (
    runid varchar primary key references chm(runid),
    flow FLOAT,
    pressure FLOAT,
    );
    insert into bin_pump_mech_params
        select
            distinct runid,
            flow,
            pressure
        from
            timetables
        order by
            runid;
    """
    )


def create_channels(
    con: db.DuckDBPyConnection,
) -> None:
    con.sql(
        """--sql
    CREATE TABLE
    channels (
        runid varchar references chm(runid),
        time double not null,
        channel varchar not null,
        percent double not null,
        primary key (runid, time, channel)
    );
    insert into channels
        unpivot (
            select
                runid,
                time,
                a,
                b,
            from
                timetables
                )
        on
            a, b
        into
            name
                channel
            value
                percent
        order by
            runid,
            time,
            channel
"""
    )


def create_solvents(con: db.DuckDBPyConnection) -> None:
    con.execute(
        """--sql
    CREATE TABLE solvents (
        runid varchar primary key references chm(runid),
        a varchar,
        b varchar,
    );
    insert into solvents
        pivot
            (select
                runid,
                lower(channel) as channel,
                ch1_solv
            from
                solvcomps
                )
        on
            channel
        using
            first(ch1_solv)
        order by
            runid
    """
    )


def load_solvprop_over_time(
    con: db.DuckDBPyConnection, overwrite: bool = False
) -> None:
    if overwrite:
        con.execute(
            """--sql
        drop table if exists solvprop_over_time;
        """
        )
    con.execute(
        """--sql
        CREATE TABLE solvprop_over_time (
            runid varchar references chm(runid),
            time double,
            channel varchar,
            percent double,
            primary key (runid, time, channel)
        );

        INSERT INTO solvprop_over_time (
            with solvcomp_subset as (
            select
                runid,
                CAST(0 as double) as time,
                lower(channel) as channel,
                percent
            FROM
                solvcomps
            ORDER BY
                runid,
                time,
                channel
            )
            select
                runid,
                time,
                channel,
                percent
            from channels
            UNION
                select
                    runid,
                    time,
                    channel,
                    percent
                from
                    solvcomp_subset
            )
        """
    )


def create_solvprop_over_time(
    con: db.DuckDBPyConnection, overwrite: bool = False
) -> None:
    create_channels(con=con)
    load_solvprop_over_time(con=con, overwrite=overwrite)


def normalise_bin_pump_tbls(
    con: db.DuckDBPyConnection, overwrite: bool = False
) -> None:
    create_pump_mech_params(con=con, overwrite=overwrite)
    create_solvents(con=con)
    create_solvprop_over_time(con=con, overwrite=overwrite)

    # clean up the normalised tables
    con.sql(
        """--sql
    DROP TABLE if exists channels;
    drop table if exists timetables;
    drop table if exists solvcomps;
    """
    )
