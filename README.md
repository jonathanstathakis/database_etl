# README

This package provides two ETL pipelines - first from my experimental data (chemstation, sampletracker, cellartracker) to a database, and secondly from the database to an xarray Dataset. Note that the second pipeline is dependent on the database structure generated through the first.

To initialize the database import `database_etl.etl_pipeline_raw` and run, providing arguments as required.

To initialize the xarray Dataset, call `database_etl.get_data` with 'xr' argument.

## Other Notes

### Cleaning ST Names

The original ST wine names / vintages entered do not in the majority of cases match the Cellar Tracker names/vintages. As these constitute the primary key between the two tables, need to form matches between each. This can be done by running 'correcting_sampletracker_name.ipynb'. It does not directly fit into the overall r ETL pipeline because I manually replaced the names in the source sampletracker sheet/file but has been kept as a reference.

