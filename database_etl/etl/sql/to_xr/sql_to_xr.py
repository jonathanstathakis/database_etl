import numpy as np
import duckdb as db
import xarray as xr
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def get_sample_metadata(con: db.DuckDBPyConnection) -> pd.DataFrame:
    """
    return each run's metadata as determined by its unique id.
    """
    return con.sql(
        """--sql
    select
        chm.id as id,
        chm.acq_date as acq_date,
        chm.acq_method as acq_method,
        chm.seq_name as seq_name,
        chm.description as description,
        st.detection as detection,
        ct.vintage::integer as vintage,
        ct.wine as wine,
        ct.locale as locale,
        ct.country as country,
        ct.region as region,
        ct.subregion as subregion,
        ct.appellation as appellation,
        ct.producer as producer,
        ct.type as type,
        ct.color as color,
        ct.category as category,
        ct.varietal as varietal
    from    
        chm
    left join
        st
    on
        chm.st_samplecode = st.samplecode
    left join
        ct
    on
        ct.vintage = st.vintage
    and
        st.wine = ct.wine
    anti join
        excluded exc
    on
        chm.samplecode = exc.samplecode
    """
    ).df()


def get_paths(con: db.DuckDBPyConnection) -> list[str]:
    """
    get the run cs img file path from the previously created `inc_image_stats` table.
    """
    return [
        path[0]
        for path in con.sql(
            """--sql
    select
        path
    from
        inc_img_stats
    """
        ).fetchall()
    ]


def add_samplecodes_to_images(
    img: pd.DataFrame, con: db.DuckDBPyConnection
) -> pd.DataFrame:
    """
    join the img file to chm to add the samplecode
    """
    return con.sql(
        """--sql
    select
        chm.samplecode,
        img.*
    from
        img
    left join
        chm
    on
        chm.id = img.id
    """
    ).df()


def fetch_imgs(con: db.DuckDBPyConnection) -> list[pd.DataFrame]:
    """
    parse the parquet file of each run's chromatospectral image
    """
    # get the img file paths
    paths = get_paths(con=con)

    # read the img file for each sample

    return [
        add_samplecodes_to_images(img=pd.read_parquet(path), con=con)
        .set_index("time")
        .rename_axis("wavelength", axis=1)
        for path in paths
    ]


def trim_times(imgs: list[pd.DataFrame], m: int) -> list[pd.DataFrame]:
    """
    trim the last row off of samples identified in longer samp
    """

    # assume that all samples have length m or greater, raise an error a sample doesnt

    for img in imgs:
        if img.shape[0] < m:
            raise ValueError(f"{img.samplecode[0]}")
    # check that all samples have the same dim sizes

    equal_length_samples = [img.iloc[0:m] for img in imgs]
    # get each row and column count as a tuple
    distinct_m = set(tuple(x.shape[0] for x in equal_length_samples))

    # assert that the set size of each equals 1
    assert len(distinct_m) == 1, f"{m}"

    return equal_length_samples


def smooth_time(df: pd.DataFrame) -> pd.DataFrame:
    """
    relabel the time index of the df to the observation frequency evenly spaced, from
    0 to the last value of the index. Assumes that the index is the time dimension,
    in minute units and that the last value of the index is the maximum.
    """
    old_index = df.index.to_numpy()
    old_index.sort()
    time_max = old_index.max()
    mean_timestep = np.round(np.mean(np.diff(old_index)), 9)

    # the slicing is due to a difficult to fix discrepency in samples with 1 more observation than the rest. Just makes sure tht the lengths of the indexes is the same. It may cause errors downstream..
    new_index = pd.Index(
        np.arange(0, time_max + mean_timestep, mean_timestep), name="mins"
    )[0 : len(old_index)]

    if not len(new_index) == len(old_index):
        raise ValueError(
            f"{len(old_index), len(new_index)}, {max(old_index), max(new_index)}"
        )

    df.index = new_index
    return df


def align_df_times(dfs: list[pd.DataFrame], m: int) -> list[pd.DataFrame]:
    """
    trim the images in `dfs` to a common maximum length then smooth so all time labels
    are row-wise equal
    """
    trimmed_df = trim_times(imgs=dfs, m=m)
    return [smooth_time(df) for df in trimmed_df]


def img_to_xr_dset(id: str, data_dict: dict):
    """ """
    df = data_dict["data"]

    return xr.Dataset(
        data_vars={"img": df},
        coords={
            **{k: [v] for k, v in data_dict.items() if k != "data"},
            **{"id": [id]},
        },
    )


def get_metadata_as_dict(con: db.DuckDBPyConnection) -> dict:
    metadata = get_sample_metadata(con=con)

    return metadata.set_index("id").to_dict(orient="index")


def get_imgs_as_dict(con: db.DuckDBPyConnection, m: int) -> dict:
    """
    fetch all chromatospectral images for all samples in included chm returned as a
    dict with the 'chm.id' as the keys and the image dataframe as the value.
    """
    imgs = fetch_imgs(con=con)
    time_aligned_imgs = align_df_times(imgs, m=m)
    return {
        img["id"][0]: img.drop(["id", "samplecode"], axis=1)
        for img in time_aligned_imgs
    }


def sql_to_xr(con: db.DuckDBPyConnection, m: int = 7800) -> xr.Dataset:
    """
    Convert the sql-stored dataset into an xarray DataSet with each chm run labeled by
    its unique 'id'.
    :m: is the restriction of the number of rows across the data.
    """

    logger.info("sql_to_xr..")

    imgs_as_dict = get_imgs_as_dict(con=con, m=m)
    metadata_as_dict = get_metadata_as_dict(con=con)
    dataset_dict = {
        id: {"data": img, **metadata_as_dict[id]} for id, img in imgs_as_dict.items()
    }

    return xr.concat(
        [img_to_xr_dset(id, d) for id, d in dataset_dict.items()], dim="id"
    )
