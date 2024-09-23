CREATE TABLE IF NOT EXISTS timetables (
    pk INTEGER REFERENCES chm (pk),
    idx INTEGER NOT NULL,
    time FLOAT NOT NULL,
    a FLOAT NOT NULL,
    b FLOAT NOT NULL,
    flow FLOAT NOT NULL,
    pressure FLOAT NOT NULL,
    PRIMARY KEY (pk, idx)
);

INSERT INTO timetables
SELECT
    chm.pk,
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
    chm.pk;
