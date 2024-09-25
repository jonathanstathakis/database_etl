CREATE TABLE IF NOT EXISTS timetables (
    samplecode varchar REFERENCES chm (samplecode),
    idx integer NOT NULL,
    time float NOT NULL,
    a float NOT NULL,
    b float NOT NULL,
    flow float NOT NULL,
    pressure float NOT NULL,
    PRIMARY KEY (samplecode, idx)
);

INSERT INTO timetables
SELECT
    chm.samplecode,
    tt.idx,
    tt.time,
    tt.a,
    tt.b,
    tt.flow,
    tt.pressure
FROM
    timetable AS tt
INNER JOIN
    chm
    ON
        tt.id = chm.id
ORDER BY
    chm.samplecode;
