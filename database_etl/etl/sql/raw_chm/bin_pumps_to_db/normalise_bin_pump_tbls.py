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
    samplecode varchar primary key references chm(samplecode),
    flow FLOAT,
    pressure FLOAT,
    );
    insert into bin_pump_mech_params
        select
            distinct samplecode,
            flow,
            pressure
        from
            timetables
        order by
            samplecode;
    """
    )


def create_channels(
    con: db.DuckDBPyConnection,
) -> None:
    con.sql(
        """--sql
    CREATE TABLE
    channels (
        samplecode varchar references chm(samplecode),
        time double not null,
        channel varchar not null,
        percent double not null,
        primary key (samplecode, time, channel)
    );
    insert into channels
        unpivot (
            select
                samplecode,
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
            samplecode,
            time,
            channel
"""
    )


def create_solvents(con: db.DuckDBPyConnection) -> None:
    con.execute(
        """--sql
    CREATE TABLE solvents (
        samplecode varchar primary key references chm(samplecode),
        a varchar,
        b varchar,
    );
    insert into solvents
        pivot
            (select
                samplecode,
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
            samplecode
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
            samplecode varchar references chm(samplecode),
            time double,
            channel varchar,
            percent double,
            primary key (samplecode, time, channel)
        );

        INSERT INTO solvprop_over_time (
            with solvcomp_subset as (
            select
                samplecode,
                CAST(0 as double) as time,
                lower(channel) as channel,
                percent
            FROM
                solvcomps
            ORDER BY
                samplecode,
                time,
                channel
            )
            select
                samplecode,
                time,
                channel,
                percent
            from channels
            UNION
                select
                    samplecode,
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
