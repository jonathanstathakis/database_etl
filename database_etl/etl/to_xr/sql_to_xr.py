import xarray as xr
import pandas as pd
import logging

logger = logging.getLogger(__name__)


def trim_times(imgs: list[pd.DataFrame], m: int) -> list[pd.DataFrame]:
    """
    trim the last row off of samples identified in longer samp
    """

    # assume that all samples have length m or greater, raise an error a sample doesnt

    for img in imgs:
        if img.shape[0] < m:
            raise ValueError(f"{img['runid'][0]}")
    # check that all samples have the same dim sizes

    equal_length_samples = [img.iloc[0:m] for img in imgs]
    # get each row and column count as a tuple
    distinct_m = set(tuple(x.shape[0] for x in equal_length_samples))

    # assert that the set size of each equals 1
    assert len(distinct_m) == 1, f"{m}"

    return equal_length_samples


def img_to_xr_dset(id: str, data_dict: dict):
    """ """
    df = data_dict["data"]

    return xr.Dataset(
        data_vars={"img": df.set_index("mins").rename_axis("wavelength", axis=1)},
        coords={
            **{k: [v] for k, v in data_dict.items() if k != "data"},
            **{"id": [id]},
        },
    )


def data_dicts_to_xr(img_dict, metadata_dict, m: int = 7800) -> xr.Dataset:
    """
    Convert the sql-stored dataset into an xarray DataSet with each chm run labeled by
    its unique 'id'.

    need to seperate the getting from the preparing

    :m: is the restriction of the number of rows across the data.
    """

    logger.info("sql_to_xr..")
    
    pd_imgs = [img.to_pandas() for img in img_dict.values()]

    trimmed_imgs = trim_times(imgs=pd_imgs, m=m)

    dataset_dict = {
        id: {"data": img, **metadata_dict[id]}
        for id, img in dict(zip(img_dict.keys(), trimmed_imgs)).items()
    }

    return xr.concat(
        [img_to_xr_dset(id, d) for id, d in dataset_dict.items()], dim="id"
    )
