from datetime import datetime
import shutil
import rainbow as rb
from pathlib import Path
import polars as pl
import pandas as pd


def get_data(path: Path) -> tuple[pl.DataFrame, pl.DataFrame]:
    datadir = rb.read(str(Path(path)))

    # image
    uv_data = datadir.get_file(filename="DAD1.UV")
    xlabels = uv_data.xlabels.ravel()
    ylabels = uv_data.ylabels
    data = (
        pd.DataFrame(index=xlabels, columns=ylabels, data=uv_data.data)
        .rename_axis("time")
        .rename_axis("nm", axis=1)
        .reset_index()
        .pipe(pl.from_pandas)
        .with_columns(pl.col("time").sub(pl.col("time").first()))
        .with_columns(
            pl.lit(datadir.metadata["id"]).alias("id"), pl.exclude(["id", "time"])
        )
        .select("id", "time", pl.exclude(["id", "time"]))
    )

    # run metadata
    metadata = {**uv_data.metadata, **datadir.metadata}

    metadata = {
        key.lower().replace(" ", "_"): val
        for key, val in metadata.items()
        if key not in ["unit", "vendor", "signal"]
    }
    return pl.DataFrame(metadata), data


def write_extraction_dir(extract_path_prefix, data, metadata) -> str:
    time_now = datetime.now().isoformat(timespec="seconds").replace(":", "")
    extract_dir = extract_path_prefix.parent / (
        str(extract_path_prefix.name) + f"{time_now}"
    )
    extract_dir.mkdir()
    metadata_out = extract_dir / "metadata.parquet"
    data_out = extract_dir / "data.parquet"

    metadata.write_parquet(metadata_out)
    data.write_parquet(data_out)

    return f"wrote extracted data to {extract_dir}"


x = 0


def extract_run_data(path: Path, overwrite=True):
    """
    Extract the metadata and data of each .D in `path` as parquets stored within the .D file under "extract_<current datetime>".

    :overwrite: if True, will overwrite the existing "extract_" dir, if False, will throw and error if a dir "extract_*" is detected in the .D dir.
    """
    metadata, data = get_data(path=path)

    dir_pattern = "extract_"

    old_dir_glob = list(path.glob(f"{dir_pattern}*"))

    old_dirpath = old_dir_glob[0]

    if not old_dirpath:
        return write_extraction_dir(
            extract_path_prefix=path / dir_pattern, metadata=metadata, data=data
        )
    elif len(old_dir_glob) > 1:
        raise ValueError(
            "multiple dirs with 'extract_' pattern detected. Please remove"
        )
    elif old_dir_glob and overwrite:
        shutil.rmtree(old_dirpath, ignore_errors=False)
        return write_extraction_dir(
            extract_path_prefix=path / dir_pattern, metadata=metadata, data=data
        )
    elif old_dirpath and not overwrite:
        raise ValueError(
            "old extraction dir detected, set overwrite = True to overwrite"
        )
    else:
        raise RuntimeError("unexpected logic path encountered")
