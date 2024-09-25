import duckdb as db
from pathlib import Path
import logging

ROOT = Path(__file__).parent


logger = logging.getLogger(__name__)


def insert_st_query():
    return ROOT / "load_raw_st.sql"


def create_st_query():
    return ROOT / "./create_st.sql"


def create_st_loading_query():
    return ROOT / "./create_st_loading.sql"


def find_wines_not_in_ct(con: db.DuckDBPyConnection):
    """
    prior to attempting insert, find any wines not in ct and store them in a seperate table 'st_not_in_ct'. Assumes that `st_loading` is already created.
    """

    con.execute(
        """--sql
    create view st_not_in_ct as
        select
            *
        from
            st_loading st
        anti join
            ct
        on
            st.vintage = ct.vintage
        and
            ct.wine = st.wine
    """
    )

    return con.sql("select count(*) from st_not_in_ct")


def exc_query(con: db.DuckDBPyConnection, path: str | Path):
    with open(path) as f:
        return con.execute(f.read())


def clean_load_raw_st(
    con: db.DuckDBPyConnection, dirty_st_path: Path, overwrite: bool = False
) -> None:
    """
    clean and load sampletracker in database.

    As st - ct joins depend on correct wine and vintage, we need to seperate samples
    that have matches and those who dont.
    """
    logger.info("clean_load_raw_st..")
    # used in query
    if overwrite:
        con.sql(
            """
            drop table if exists st cascade;
            drop view if exists st_not_in_ct cascade;
            drop view if exists st_loading cascade;
            drop sequence if exists pk_st_seq cascade;
            """
        )

    # create st table
    exc_query(con=con, path=create_st_query())

    # get the sampletracker data from the file
    con.execute("set variable dirty_st_path = ?", parameters=[str(dirty_st_path)])

    # create the interm. loading table
    exc_query(con=con, path=create_st_loading_query())

    # find wines not in ct, add to new table `st_not_in_ct`
    find_wines_not_in_ct(con=con)

    # insert into st table
    exc_query(con=con, path=insert_st_query())

    # delete the filepath var from the db
    con.execute("reset variable dirty_st_path")
    con.execute("drop view st_loading")
