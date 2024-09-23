




-- create the primary key table to check against
-- See: <https://stackoverflow.com/questions/72883083/create-an-auto-incrementing-primary-key-in-duckdb>

COMMENT ON TABLE
    bin_pump.id
IS
    'A primary key table for the solvcomps and timetable tables as they are stored in blocks. Use num to join the blocks to the ids for samplewise extraction';

COMMENT ON COLUMN
    bin_pump.id.id
IS
    'sample primary key. Use in association with num to get a samples solvcomp or timetable from those tables';

COMMENT ON COLUMN
    bin_pump.id.num
IS
    'the numerical order of the samples added to the tables in this schema (bin_pump). Used to extract a samples table from solvcomps or timetables through joining. i.e. join the sample to the id in this table, use this num to join to the data tables.';

COMMENT ON TABLE
    bin_pump.timetables
IS
    'The sample solvent timetables - information about the elution, whether it was gradient, etc. In wide format with samples stacked vertically, use num and id in bin_pump.id to extract samplewise.';

COMMENT ON TABLE
    bin_pump.solvcomps
IS
    'Information about a samples solvent composition. In wide format with samples stacked vertically, use num and id in bin_pump.id to extract samplewise.';