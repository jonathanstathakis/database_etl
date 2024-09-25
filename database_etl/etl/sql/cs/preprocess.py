"""
Preprocessing methods
"""
import pandas as pd
import numpy as np

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