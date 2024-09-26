"""
import `etl_pipeline_raw` to transform a dir of .D chemstation dirs to a xarray Dataset.
"""

from .etl.etl_pipeline_raw import (
    etl_pipeline_raw as etl_pipeline_raw,
    get_data as get_data,
)
