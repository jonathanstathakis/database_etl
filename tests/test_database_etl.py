"""
Use fixtures to manage pipeline dependencies, execution order. Table A needs table B, so
 a fixture creating table B is included in the fixture creating table A, but not necessarily
 called, just included in the parameters.
"""

from pathlib import Path

import duckdb as db
import pytest
import xarray as xr
from xarray import testing as xr_test

from database_etl.definitions import RAW_DATA_LIB, DIRTY_ST_PATH
from database_etl import definitions
from database_etl.etl.sql.cs import load_image_stats
from database_etl.etl.sql.ct import load_ct
from database_etl.etl.etl_pipeline_raw import (
    etl_pipeline_raw,
    get_data,
)
from database_etl.etl.sql.raw_chm import load_chm
from database_etl.etl.sql.raw_chm.bin_pumps_to_db.bin_pump_to_db_ import bin_pump_to_db
from database_etl.etl.sql.raw_chm.bin_pumps_to_db.normalise_bin_pump_tbls import (
    normalise_bin_pump_tbls,
)
from database_etl.etl.sql.raw_st import clean_load_raw_st


@pytest.fixture(scope="module")
def test_data_dir():
    path = (Path(__file__).parent / ".." / "tests" / "test_data").resolve()

    assert path
    assert path.exists()
    assert path.is_dir()
    assert list(path.glob("*"))
    return path


@pytest.fixture(scope="module")
def test_d_paths(test_data_dir) -> list[Path]:
    result = list(test_data_dir.glob("*.D"))

    if result:
        return result
    else:
        raise ValueError("no D found")


@pytest.fixture(scope="module")
def ct_un():
    return definitions.CT_UN


@pytest.fixture(scope="module")
def ct_pw():
    return definitions.CT_PW


@pytest.fixture(scope="module")
def clean_ct_db() -> str:
    return "./tests/clean_ct_db"


@pytest.fixture(scope="module")
def testcon() -> db.DuckDBPyConnection:
    return db.connect()


@pytest.fixture(scope="module")
def exc_load_ct(ct_un, ct_pw, testcon: db.DuckDBPyConnection) -> db.DuckDBPyConnection:
    """
    test if load ct can write to db
    """

    load_ct(un=ct_un, pw=ct_pw, con=testcon, output="db", overwrite=True)
    result = testcon.sql(
        """--sql
        select count(*) > 0 from ct
        """
    ).fetchone()

    if result:
        assert result[0]

    return testcon


def test_load_clean_ct(exc_load_ct: db.DuckDBPyConnection) -> None:
    assert exc_load_ct


@pytest.fixture(scope="module")
def dirty_st_path() -> Path:
    return Path(DIRTY_ST_PATH)


@pytest.fixture(scope="module")
def exc_load_raw_st(
    testcon: db.DuckDBPyConnection, dirty_st_path
) -> db.DuckDBPyConnection:
    """
    test if `clean_load_st` can write to db
    """
    clean_load_raw_st(con=testcon, dirty_st_path=dirty_st_path, overwrite=True)

    result = testcon.execute("select count(*) > 0 from st").fetchone()

    if result:
        assert result[0]

    return testcon


def test_clean_load_raw_st(
    exc_load_ct: db.DuckDBPyConnection, exc_load_raw_st: db.DuckDBPyConnection
) -> None:
    assert exc_load_raw_st


@pytest.fixture(scope="module")
def db_w_ct_st(
    exc_load_ct: db.DuckDBPyConnection,
    exc_load_raw_st: db.DuckDBPyConnection,
):
    """
    ensure that both fixtures `db_w_clean_ct` and `db_w_clean_st` are executed, check
    that the tables are present as expected, and return one of the objects, shouldnt matter
    which.
    """
    tables = [result[0] for result in exc_load_raw_st.execute("show tables").fetchall()]

    assert sorted(["ct", "st", "st_not_in_ct"]) == sorted(tables)
    return exc_load_raw_st


def test_db_w_ct_st(db_w_ct_st: db.DuckDBPyConnection):
    assert db_w_ct_st


def test_get_metadata_file_paths() -> None:
    """
    test whether `etl.sql.raw_chm.clean_load_raw_chm` works
    """

    from database_etl.etl.sql.raw_chm.clean_load_raw_chm import get_metadata_file_paths

    result = get_metadata_file_paths(RAW_DATA_LIB)

    assert result


@pytest.mark.skip("takes a while to run")
def test_extract_ch(test_d_paths: list[Path]) -> None:
    """
    create a new dir with the extracted ch data
    """
    from database_etl.etl.ch_extractor import extract_run_data

    results = []
    for path in test_d_paths:
        results.append(extract_run_data(path=path, overwrite=True))

    assert results


@pytest.fixture(scope="module")
def exc_load_chm(
    db_w_ct_st: db.DuckDBPyConnection, test_data_dir: Path
) -> db.DuckDBPyConnection:
    """
    test creation of metadata and sequences tables
    """
    # need st in db

    load_chm(con=db_w_ct_st, lib_dir=test_data_dir, overwrite=True)

    result = db_w_ct_st.execute("select count(*) > 0 from chm").fetchone()

    if result:
        assert result

    return db_w_ct_st


def test_extracted_metadata_to_db(
    exc_load_chm: db.DuckDBPyConnection,
) -> None:
    """
    Test `clean_load_raw_chm`. Logic is in the fixture
    """
    assert exc_load_chm


@pytest.fixture(scope="module")
def db_w_ct_st_chm_bin_pumps(
    exc_load_chm: db.DuckDBPyConnection, test_d_paths: list[Path]
) -> db.DuckDBPyConnection:
    bin_pump_to_db(paths=test_d_paths, con=exc_load_chm, overwrite=True)

    return exc_load_chm


def test_bin_pump_to_db(db_w_ct_st_chm_bin_pumps: db.DuckDBPyConnection) -> None:
    """
    test the generation of the intermediate 'colvcomps' and 'timetables' tables parsed from
    the binary pump data
    """
    assert db_w_ct_st_chm_bin_pumps

    if result := db_w_ct_st_chm_bin_pumps.execute(
        "select count(*) > 0 from solvcomps"
    ).fetchone():
        assert result[0]
    if result := db_w_ct_st_chm_bin_pumps.execute(
        "select count(*) > 0 from timetables"
    ).fetchone():
        assert result[0]


@pytest.fixture(scope="module")
def db_w_ct_st_chm_norm_bin_pump(
    db_w_ct_st_chm_bin_pumps: db.DuckDBPyConnection,
) -> db.DuckDBPyConnection:
    """
    db with normalised binary pump tables
    """

    normalise_bin_pump_tbls(con=db_w_ct_st_chm_bin_pumps, overwrite=True)

    tables = [
        result[0]
        for result in db_w_ct_st_chm_bin_pumps.execute(
            "select name from (show tables)"
        ).fetchall()
    ]

    expected_tables = ["bin_pump_mech_params", "solvents", "solvprop_over_mins"]

    for table in expected_tables:
        assert table in tables

        result = db_w_ct_st_chm_bin_pumps.execute(
            f"select count(*) > 0 from {table}"
        ).fetchone()
        if result:
            assert result[0]
        else:
            raise ValueError("no values returned")

    return db_w_ct_st_chm_bin_pumps


def test_normalise_bin_pump_tbls(
    db_w_ct_st_chm_norm_bin_pump: db.DuckDBPyConnection,
) -> None:
    assert db_w_ct_st_chm_norm_bin_pump


@pytest.fixture(scope="module")
def exc_get_sample_gradients(
    db_w_ct_st_chm_norm_bin_pump: db.DuckDBPyConnection,
) -> db.DuckDBPyConnection:
    from database_etl.etl.sql.raw_chm.bin_pumps_to_db import get_sample_gradients

    get_sample_gradients(con=db_w_ct_st_chm_norm_bin_pump)

    return db_w_ct_st_chm_norm_bin_pump


def test_get_sample_gradients(exc_get_sample_gradients: db.DuckDBPyConnection) -> None:
    assert exc_get_sample_gradients
    result = exc_get_sample_gradients.execute(
        "select count(*) > 0 from counts_per_gradient"
    ).fetchone()
    if result:
        assert result[0]
    else:
        raise ValueError("no result returned")


@pytest.fixture(scope="module")
def exc_load_image_stats(
    testcon: db.DuckDBPyConnection,
    test_data_dir: Path,
) -> db.DuckDBPyConnection:
    load_image_stats(lib_path=test_data_dir, con=testcon, overwrite=True)

    return testcon


def test_load_image_stats(
    exc_load_chm: db.DuckDBPyConnection,
    exc_load_image_stats: db.DuckDBPyConnection,
) -> None:
    if result := exc_load_chm.execute(
        "select count(*) > 0 from image_stats"
    ).fetchone():
        assert result[0]
    else:
        raise ValueError("no result returned")

    assert exc_load_image_stats


@pytest.fixture(scope="module")
def excluded_samples() -> list[dict[str, str]]:
    return [{"runid": "54", "reason": "test"}]


@pytest.fixture(scope="module")
def exc_gen_excluded_inc(
    exc_load_chm: db.DuckDBPyConnection,
    exc_load_image_stats: db.DuckDBPyConnection,
    excluded_samples: list[dict[str, str]],
) -> db.DuckDBPyConnection:
    """
    execute `gen_excluded_tbl_inc_chm_view_inc_img_view` to generate the `excluded` tbl and associated views
    """

    from database_etl.etl.sql.excluded import gen_included_views

    gen_included_views(
        con=exc_load_image_stats, overwrite=True, excluded_samples=excluded_samples
    )

    return exc_load_image_stats


def test_gen_excluded_tbl_inc_chm_view_inc_img_view(
    exc_gen_excluded_inc: db.DuckDBPyConnection,
) -> None:
    """
    test that the fixture works, test that the `new_objs` expected to be created are,
    test that each of the objs have rows
    """
    assert exc_gen_excluded_inc
    new_objs = ["excluded", "inc_chm", "inc_img_stats"]

    for obj in new_objs:
        assert (
            obj in exc_gen_excluded_inc.execute("select name from (show)").pl()["name"]
        )

        if count_gt_0 := exc_gen_excluded_inc.execute(
            f"select count(*) > 0 from {obj}"
        ).fetchone():
            assert count_gt_0[0], obj
        else:
            raise ValueError(f"{obj} has no rows")


@pytest.fixture(scope="module")
def expected_tables() -> list[str]:
    return [
        "bin_pump_mech_params",
        "chm",
        "counts_per_gradient",
        "ct",
        "excluded",
        "gradients",
        "image_stats",
        "run_data_paths",
        "sequences",
        "solvents",
        "solvprop_over_mins",
        "st",
    ]


@pytest.fixture(scope="module")
def exc_etl_pipeline_raw_mock(
    test_data_dir: Path,
    dirty_st_path: Path,
    ct_pw: str,
    ct_un: str,
    expected_tables: list[str],
    excluded_samples: list[dict[str, str]],
    testcon: db.DuckDBPyConnection = db.connect(),
) -> db.DuckDBPyConnection:
    """
    Test the execution of the full pipeline
    """

    etl_pipeline_raw(
        data_dir=test_data_dir,
        con=testcon,
        dirty_st_path=dirty_st_path,
        ct_pw=ct_pw,
        ct_un=ct_un,
        run_extraction=False,
        excluded_samples=excluded_samples,
        overwrite=True,
    )

    # check that the expected tables are in the database and not empty

    error_tables = []

    for table in expected_tables:
        try:
            testcon.execute(
                f"""--sql
            select
                count(*) > 0
            from
                {table}
            """
            ).pl()
        except Exception as e:
            error_tables.append((table, e))

    if error_tables:
        raise RuntimeError("the following errors were encountered:\n", error_tables)

    return testcon


@pytest.fixture(scope="module")
def test_dset_path(test_data_dir: Path) -> Path:
    return test_data_dir / "test_dset.nc"


@pytest.fixture(scope="module")
def xr_dset_mock(exc_etl_pipeline_raw_mock: db.DuckDBPyConnection) -> xr.Dataset:
    result = get_data(output="xr", con=exc_etl_pipeline_raw_mock)
    if result:
        if isinstance(result, xr.Dataset):
            dset = result
        else:
            raise TypeError("expected xr.Dataset")

        assert dset
        return result
    else:
        raise RuntimeError


def test_xr_dset_raw(xr_dset_mock: xr.Dataset, test_dset_path: Path) -> None:
    xr_test.assert_equal(xr_dset_mock, xr.open_dataset(test_dset_path))


def test_etl_pipeline_raw_full_dset(
    dirty_st_path: Path,
    ct_pw: str,
    ct_un: str,
    excluded_samples: list[dict[str, str]] = [
        {
            "runid": "2021-debortoli-cabernet-merlot_avantor",
            "reason": "aborted run",
        }
    ],
    full_lib_dir: Path = Path("../../../jonathan/uni/0_jono_data/raw_uv").resolve(),
    run_extraction: bool = True,
) -> None:
    etl_pipeline_raw(
        run_extraction=run_extraction,
        data_dir=full_lib_dir,
        dirty_st_path=dirty_st_path,
        ct_pw=ct_pw,
        ct_un=ct_un,
        excluded_samples=excluded_samples,
    )


@pytest.fixture
def data_tuple(exc_etl_pipeline_raw_mock: db.DuckDBPyConnection) -> tuple:
    """
    Extracted data as a tuple
    """
    dtup = get_data(output="tuple", con=exc_etl_pipeline_raw_mock)
    return dtup


def test_get_data_as_tuple(data_tuple: tuple) -> None:
    """
    test that the tuples are produced and that each item has the correct runids
    """
    assert data_tuple

    for pair in data_tuple:
        img = pair[0]
        mta = pair[1]

        assert img["runid"][0] == mta["runid"][0]


def test_get_data_select_samples(
    exc_etl_pipeline_raw_mock: db.DuckDBPyConnection, test_d_paths
):
    """
    use get_data to get only the samples specified
    """

    mock_runids = ["13", "54", "61", "89"]
    removed_code = "13"
    data = get_data(
        output="tuple",
        con=exc_etl_pipeline_raw_mock,
        runids=[x for x in mock_runids if x != removed_code],
    )
    assert data

    for pair in data:
        img = pair[0]
        mta = pair[1]

        assert img["runid"][0] == mta["runid"][0]

        assert img["runid"][0] != removed_code


def test_write_mock_db_persist(test_data_dir, dirty_st_path, ct_pw, ct_un):
    path = Path(__file__).parent / "testdb"
    persist_db = db.connect(path)
    etl_pipeline_raw(
        data_dir=test_data_dir,
        dirty_st_path=dirty_st_path,
        ct_un=ct_un,
        ct_pw=ct_pw,
        con=persist_db,
    )

    db.close()
    path.unlink()
