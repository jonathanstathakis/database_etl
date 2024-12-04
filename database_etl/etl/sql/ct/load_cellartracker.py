from cellartracker import cellartracker
import pandas as pd
from pathlib import Path
import duckdb as db
import html

import logging

logger = logging.getLogger(__name__)


def get_clean_ct(
    un: str,
    pw: str,
    output: str,
    con: None | db.DuckDBPyConnection,
    parquet_path="",
    overwrite: bool = False,
):
    """
    Possible output options:
        -'df': returns a pandas dataframe
        - 'parquet': outputs the clean table the given `parquet_path`
        - 'db' does nothing after loading the clean table into the connected db
    """
    logger.info("get_clean_ct..")
    output_opts = ["df", "parquet", "db"]
    if output not in output_opts:
        raise ValueError(f"please set `output` equal to one of {output_opts}")

    if not con:
        con = db.connect()

    if overwrite:
        con.execute("drop table if exists ct; drop sequence if exists ct_pk_seq;")
    dirty_ct = dl_ct(un=un, pw=pw)

    match output:
        case "df":
            con = db.connect()
            load_clean_ct(con=con, ct_df=dirty_ct)
            return con.sql(
                """--sql
            select * from ct
            """
            ).df()
        case "parquet":
            con = db.connect()
            load_clean_ct(con=con, ct_df=dirty_ct)
            con.sql(
                f"""--sql
            copy (select * from ct) to '{parquet_path}'
            """
            )
            print(f"saved ct to {parquet_path}")
            return None
        case "db":
            load_clean_ct(con=con, ct_df=dirty_ct)
            return None


def get_load_clean_ct_query():
    return Path(__file__).parent / "clean_load_ct.sql"


def load_clean_ct(con: db.DuckDBPyConnection, ct_df: pd.DataFrame) -> None:
    ct_df["Wine"] = ct_df["Wine"].apply(html.unescape)
    with open(get_load_clean_ct_query()) as f:
        query = f.read()

    con.execute(query)


def dl_ct(un: str, pw: str):
    client = cellartracker.CellarTracker(username=un, password=pw)

    usecols = [
        "Size",
        "Vintage",
        "Wine",
        "Locale",
        "Country",
        "Region",
        "SubRegion",
        "Appellation",
        "Producer",
        "Type",
        "Color",
        "Category",
        "Varietal",
    ]

    cellar_tracker_df = pd.DataFrame(client.get_list())
    cellar_tracker_df = cellar_tracker_df[usecols]

    return cellar_tracker_df


def web_to_parquet(path: Path, un: str, pw: str, overwrite: bool = False) -> None:
    if overwrite:
        df = web_to_df(un=un, pw=pw)
        df.to_parquet(path)


def web_to_df(un: str, pw: str) -> pd.DataFrame:
    """
    get cellartracker data from web return as a df
    """
    return dl_ct(un=un, pw=pw)


def web_to_db(con: db.DuckDBPyConnection, un: str, pw: str) -> None:
    web_to_df(un=un, pw=pw)
