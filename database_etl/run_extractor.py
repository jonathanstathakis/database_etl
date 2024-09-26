from database_etl.etl.ch_extractor import extract_run_data
from database_etl.definitions import RAW_DATA_LIB

print(RAW_DATA_LIB)

paths = list(RAW_DATA_LIB.glob("*.D"))

results = []
if paths:
    for path in paths:
        results.append(extract_run_data(path, overwrite=True))
else:
    raise ValueError("no .D found")

print(results)
