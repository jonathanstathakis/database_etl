import duckdb as db
import pandas as pd
import xmltodict
from pathlib import Path
from IPython.display import display
from collections import Counter


def xml_table_to_df(table) -> pd.DataFrame:
    # sometimes its not a list
    if not isinstance(table["Row"], list):
        table["Row"] = [table["Row"]]

    row_0 = table["Row"][0]

    if not isinstance(row_0, dict):
        raise TypeError

    columns = [parameter["Name"] for parameter in row_0["Parameter"]]

    column_dict = {col: [] for col in columns}
    for row in table["Row"]:
        for param in row["Parameter"]:
            name = param["Name"]
            column_dict[name].append(param["Value"])
    df = pd.DataFrame.from_dict(column_dict)

    return df


def get_bin_pump(macaml):
    sections = macaml["ACAML"]["Doc"]["Content"]["MethodConfiguration"][
        "MethodDescription"
    ]["Section"]["Section"]
    bin_pump_idx = find_bin_pump(sections)
    bin_pump = sections[bin_pump_idx]
    return bin_pump


def get_solvent_comp(bin_pump):
    solvent_comp = bin_pump["Table"][0]
    return solvent_comp


def get_timetable(bin_pump):
    try:
        timetable = bin_pump["Table"][1]
    except KeyError as e:
        e.add_note(f"candidate keys: {bin_pump.keys()}")
        raise e
    return timetable


def format_timetable(timetable: pd.DataFrame, id: str):
    """
    add the id column and clean the existing column names
    """
    timetable["id"] = id
    timetable.columns = [col.lower() for col in timetable.columns]
    timetable = timetable.reset_index(names="idx")
    try:
        timetable = timetable[["id", "idx", "time", "a", "b", "flow", "pressure"]]
    except KeyError as e:
        e.add_note(str(timetable.columns))
        raise e
    return timetable


def format_solvent_comp(solvent_comp: pd.DataFrame, id: str):
    """
    add the id column and clean the existing column names
    """
    solvent_comp["id"] = id
    solvent_comp.columns = [col.lower() for col in solvent_comp.columns]
    solvent_comp = solvent_comp.reset_index(names="idx")
    solvent_comp = solvent_comp.rename(
        {
            "ch. 1 solv.": "ch1_solv",
            "ch2 solv.": "ch2_solv",
            "name 1": "name_1",
            "name 2": "name_2",
        },
        axis=1,
    )
    try:
        solvent_comp = solvent_comp[
            [
                "id",
                "idx",
                "channel",
                "ch1_solv",
                "name_1",
                "ch2_solv",
                "name_2",
                "selected",
                "used",
                "percent",
            ]
        ]
    except KeyError as e:
        e.add_note(str(solvent_comp.columns))
        raise e
    return solvent_comp


def open_xml(path: str):
    with open(path, "rb") as f:
        xmldict = xmltodict.parse(f)
    return xmldict


def get_tables(bin_pump) -> tuple[dict, dict]:
    if not isinstance(bin_pump, dict):
        raise TypeError

    if "Name" not in bin_pump:
        raise ValueError

    solvent_comp = get_solvent_comp(bin_pump)
    timetable = get_timetable(bin_pump)

    return solvent_comp, timetable


sequence_path = "/Users/jonathan/uni/0_jono_data/raw_uv/45.D/sequence.acam_"
macaml_path = "/Users/jonathan/uni/0_jono_data/raw_uv/45.D/acq.macaml"


def get_macaml_path(dpath: str):
    return str(Path(dpath) / "acq.macaml")


def validate_table(table):
    if "Row" not in table:
        return None


def find_bin_pump(sections):
    for idx, section in enumerate(sections):
        if section["Name"] == "Binary Pump":
            return idx


def get_id(dpath: Path) -> str:
    """
    get the id string from the 'sequence.acam_' or 'sample.acaml' file.

    Single runs have 'sample.acaml', sequence runs have 'sequence.acam_', however the
    XML structures appear very similar, and the ID is in the same location.
    """
    seq_acam_path = dpath / "sequence.acam_"
    sample_acaml_path = dpath / "sample.acaml"

    if seq_acam_path.exists():
        acam_ = open_xml(str(seq_acam_path))
        id = get_id_(acam_)

    elif sample_acaml_path.exists():
        acaml = open_xml(str(sample_acaml_path))
        id = get_id_(acaml)

    else:
        raise ValueError(f"cant find sequence.acam_ or sample.acaml in {dpath}")

    return id


def get_id_(sequence: dict) -> str:
    """
    Extract the 'id' string at the location given below
    """
    id = sequence["ACAML"]["Doc"]["Content"]["SampleContexts"]["Setup"]["@id"]
    return id


def get_bin_pump_tables(dpath: Path) -> tuple[pd.DataFrame, pd.DataFrame]:
    """

    part 1 of the process.

    For  a .D dir at `dpath` find either a 'sequence.acam_' or 'sample.acaml' to extract
    the 'id', and
    """

    if not isinstance(dpath, (str, Path)):
        raise TypeError("dpath must be str or Path")

    # get the id string for the sample
    id = get_id(dpath)

    # get the acq.macaml XML as a dict
    macaml_path = get_macaml_path(dpath)
    macaml = open_xml(macaml_path)

    # extract the branch containing the binary pump information
    bin_pump = get_bin_pump(macaml)

    # stract the branches containing the tables in dict form
    solvent_comp, timetable = get_tables(bin_pump)

    # parse the tables, returning them as pandas DataFrames

    # check that the tables have rows
    try:
        if "Row" in solvent_comp:
            solvent_comp_df = xml_table_to_df(solvent_comp).pipe(
                format_solvent_comp, id
            )
        else:
            solvent_comp_df = pd.DataFrame()
        if "Row" in timetable:
            timetable_df = xml_table_to_df(timetable).pipe(format_timetable, id)
        else:
            timetable_df = pd.DataFrame()
    except KeyError as e:
        e.add_note(Path(dpath).stem)
        raise e

    # confirm the tables have been created as expected.
    if timetable_df.empty or solvent_comp_df.empty:
        raise ValueError

    return solvent_comp_df, timetable_df


def load_timetables(
    timetables: dict[Path, pd.DataFrame], con: db.DuckDBPyConnection
) -> None:
    with open(Path(__file__).parent / "queries/load_timetables.sql") as f:
        load_timetables_query = f.read()

        for path, timetable in timetables.items():
            try:
                con.execute(load_timetables_query)
            except db.ConstraintException as e:
                display(path)
                display(timetable)
                display(con.sql("select * from timetables").pl())
                raise e


def load_solvcomps(
    solvcomps: dict[Path, pd.DataFrame], con: db.DuckDBPyConnection
) -> None:
    with open(Path(__file__).parent / "queries/load_solvcomps.sql") as f:
        load_solvcomps_query = f.read()

        for path, solvcomp in solvcomps.items():
            solvcomp = solvcomp
            try:
                con.execute(load_solvcomps_query)
            except db.ConstraintException as e:
                display(path)
                display(solvcomp)
                display(
                    con.sql(
                        """--sql
                select
                    *
                from
                    solvcomps
                order by
                    runid
                """
                    ).df()
                )
                raise e


def bin_pump_to_db(
    paths: list[Path],
    con: db.DuckDBPyConnection,
    overwrite: bool = False,
) -> None:
    """
    Wrapper for `get_bin_pump_tables` and `sample_tables_to_db` returning the table number
    `tbl_num` generated by each extraction.
    """

    if overwrite:
        con.execute(
            """--sql
        drop table if exists timetables cascade;
        drop table if exists solvcomps cascade;
        """
        )

    counts = Counter(paths)

    duplicate_counts = {item: count for item, count in counts.items() if count > 1}

    if duplicate_counts:
        raise ValueError("duplicate paths detected")

    if not paths:
        raise ValueError("no .D paths found")

    tables = {path: get_bin_pump_tables(path) for path in paths}

    solvcomps = {path: table[0] for path, table in tables.items()}
    timetables = {path: table[1] for path, table in tables.items()}

    load_solvcomps(solvcomps=solvcomps, con=con)
    load_timetables(timetables=timetables, con=con)
