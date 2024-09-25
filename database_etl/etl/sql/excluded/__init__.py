"""
Import `gen_excluded_tbl_inc_chm_view_inc_img_view` to generate the `excluded` table, `inc_chm` and `inc_img_stats` tbls, and add a sample, `pk`=61 to the `excluded` table.

TODO: need to refactor this into two modules: one to create the excluded object space - the excluded table and the inverted interfaces `inc_chm` and `inc_img_stats`, and another to find the outliers and add them to `excluded`. In this way the outlier finding module can generalise outlier detection processes and the architecture specific creation routine remains pure.
"""

from .excluded_tbl import (
    gen_included_views as gen_included_views,
)
