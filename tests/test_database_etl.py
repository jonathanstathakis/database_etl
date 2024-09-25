import pytest
import duckdb as db
from pathlib import Path
from database_etl.etl.sql.ct import get_clean_ct
from database_etl.etl.sql.raw_st import clean_load_raw_st
from database_etl.definitions import RAW_DATA_LIB
from database_etl.etl.sql.raw_chm import extracted_metadata_to_db
import xarray as xr
from database_etl.etl.sql.etl_pipeline_raw import etl_pipeline_raw
from xarray import testing as xr_test
from database_etl.etl.sql.raw_chm.bin_pumps_to_db.bin_pump_to_db_ import bin_pump_to_db
from database_etl.etl.sql.raw_chm.bin_pumps_to_db.normalise_bin_pump_tbls import (
    normalise_bin_pump_tbls,
)


@pytest.fixture(scope="module")
def test_data_dir():
    path = (Path(__file__).parent / ".." / "tests" / "test_data").resolve()

    assert path
    assert path.exists()
    assert path.is_dir()
    assert list(path.glob("*"))
    return path


@pytest.fixture(scope="module")
def ct_un():
    return "OctaneOolong"


@pytest.fixture(scope="module")
def ct_pw():
    return "S74rg4z3r1"


@pytest.fixture(scope="module")
def clean_ct_db() -> str:
    return "./tests/clean_ct_db"


@pytest.fixture(scope="module")
def testcon() -> db.DuckDBPyConnection:
    return db.connect()


@pytest.fixture(scope="module")
def db_w_clean_ct(
    ct_un, ct_pw, testcon: db.DuckDBPyConnection
) -> db.DuckDBPyConnection:
    """
    test if load ct can write to db
    """

    get_clean_ct(un=ct_un, pw=ct_pw, con=testcon, output="db", overwrite=True)
    result = testcon.sql(
        """--sql
        select count(*) > 0 from ct
        """
    ).fetchone()

    if result:
        assert result[0]

    return testcon


def test_load_clean_ct(db_w_clean_ct: db.DuckDBPyConnection) -> None:
    assert db_w_clean_ct


@pytest.fixture(scope="module")
def dirty_st_path() -> Path:
    return Path(
        "/Users/jonathan/mres_thesis/database_etl/database_etl/data/dirty_sample_tracker_names_corrected.parquet"
    )


@pytest.fixture(scope="module")
def exc_clean_load_raw_st(
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
    db_w_clean_ct: db.DuckDBPyConnection, exc_clean_load_raw_st: db.DuckDBPyConnection
) -> None:
    assert exc_clean_load_raw_st


@pytest.fixture(scope="module")
def db_w_ct_st(
    db_w_clean_ct: db.DuckDBPyConnection,
    exc_clean_load_raw_st: db.DuckDBPyConnection,
):
    """
    ensure that both fixtures `db_w_clean_ct` and `db_w_clean_st` are executed, check
    that the tables are present as expected, and return one of the objects, shouldnt matter
    which.
    """
    tables = [
        result[0] for result in exc_clean_load_raw_st.execute("show tables").fetchall()
    ]

    assert sorted(["ct", "st", "st_not_in_ct"]) == sorted(tables)
    return exc_clean_load_raw_st


def test_db_w_ct_st(db_w_ct_st: db.DuckDBPyConnection):
    assert db_w_ct_st


@pytest.fixture(scope="module")
def raw_ch_d_paths() -> list[Path]:
    d_dirs = ["021.D", "089.D", "061.D"]
    return [RAW_DATA_LIB / x for x in d_dirs]


def test_get_metadata_file_paths() -> None:
    """
    test whether `etl.sql.raw_chm.clean_load_raw_chm` works
    """

    from database_etl.etl.sql.raw_chm.clean_load_raw_chm import get_metadata_file_paths

    result = get_metadata_file_paths(RAW_DATA_LIB)

    assert result


# pytest.fixture(scope="module")
# def extract_ch(raw_ch_d_paths: list[Path], tmp_path_factory) -> Path:
#     """
#     create a new dir with the extracted ch data
#     """


@pytest.fixture(scope="module")
def exc_clean_extrac_mtad_to_db(
    db_w_ct_st: db.DuckDBPyConnection, test_data_dir: Path
) -> db.DuckDBPyConnection:
    """
    test creation of metadata and sequences tables
    """
    # need st in db

    extracted_metadata_to_db(con=db_w_ct_st, lib_dir=test_data_dir, overwrite=True)

    result = db_w_ct_st.execute("select count(*) > 0 from chm").fetchone()

    if result:
        assert result

    return db_w_ct_st


def test_extracted_metadata_to_db(
    exc_clean_extrac_mtad_to_db: db.DuckDBPyConnection,
) -> None:
    """
    Test `clean_load_raw_chm`. Logic is in the fixture
    """
    assert exc_clean_extrac_mtad_to_db


@pytest.fixture(scope="module")
def db_w_ct_st_chm_bin_pumps(
    exc_clean_extrac_mtad_to_db: db.DuckDBPyConnection, raw_ch_d_paths: list[Path]
) -> db.DuckDBPyConnection:
    bin_pump_to_db(
        paths=raw_ch_d_paths, con=exc_clean_extrac_mtad_to_db, overwrite=True
    )

    return exc_clean_extrac_mtad_to_db


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

    expected_tables = ["bin_pump_mech_params", "solvents", "solvprop_over_time"]

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


@pytest.fixture
def exc_load_image_stats(
    testcon: db.DuckDBPyConnection,
    test_data_dir: Path,
) -> db.DuckDBPyConnection:
    from database_etl.etl.sql.cs import load_image_stats

    load_image_stats(lib_path=test_data_dir, con=testcon, overwrite=True)

    return testcon


def test_load_image_stats(
    exc_clean_extrac_mtad_to_db: db.DuckDBPyConnection,
    exc_load_image_stats: db.DuckDBPyConnection,
) -> None:
    if result := exc_clean_extrac_mtad_to_db.execute(
        "select count(*) > 0 from image_stats"
    ).fetchone():
        assert result[0]
    else:
        raise ValueError("no result returned")

    assert exc_load_image_stats


@pytest.fixture
def excluded_samples() -> list[dict[str, str]]:
    return [{"samplecode": "54", "reason": "test"}]


@pytest.fixture
def exc_gen_excluded_inc(
    exc_clean_extrac_mtad_to_db: db.DuckDBPyConnection,
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


@pytest.fixture
def xr_from_sql(
    exc_load_image_stats: db.DuckDBPyConnection,
    exc_get_sample_gradients: db.DuckDBPyConnection,
    exc_gen_excluded_inc: db.DuckDBPyConnection,
) -> xr.Dataset:
    from database_etl.etl.sql.to_xr import sql_to_xr

    dset = sql_to_xr(con=exc_get_sample_gradients)

    assert dset

    return dset


def test_sql_to_xr(xr_from_sql: xr.Dataset) -> None:
    assert xr_from_sql


@pytest.fixture
def test_dset(test_data_dir: Path) -> xr.Dataset:
    return xr.open_dataset(test_data_dir / "test_dset.nc")


def test_etl_pipeline_raw(
    test_data_dir: Path,
    dirty_st_path: Path,
    ct_pw: str,
    ct_un: str,
    test_dset: xr.Dataset,
    excluded_samples: list[str],
    testcon: db.DuckDBPyConnection = db.connect(),
):
    """
    Test the execution of the full pipeline judged by whether the resulting xr.Dataset
    equals a stored version created from the same process.
    """
    dset: xr.Dataset = etl_pipeline_raw(
        data_dir=test_data_dir,
        con=testcon,
        dirty_st_path=dirty_st_path,
        ct_pw=ct_pw,
        ct_un=ct_un,
        run_extraction=False,
        excluded_samples=excluded_samples,
    )

    assert dset

    xr_test.assert_equal(dset, test_dset)


def test_etl_pipeline_raw_full_dset(
    dirty_st_path: Path,
    ct_pw: str,
    ct_un: str,
    excluded_samples: list[dict[str, str]] = [
        {
            "samplecode": "2021-debortoli-cabernet-merlot_avantor",
            "reason": "aborted run",
        }
    ],
    full_lib_dir: Path = Path("../../../jonathan/uni/0_jono_data/raw_uv").resolve(),
    run_extraction: bool = True,
) -> None:
    dset = etl_pipeline_raw(
        run_extraction=run_extraction,
        data_dir=full_lib_dir,
        dirty_st_path=dirty_st_path,
        ct_pw=ct_pw,
        ct_un=ct_un,
        excluded_samples=excluded_samples,
    )

    assert dset
