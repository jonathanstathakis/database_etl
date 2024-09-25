CREATE TABLE IF NOT EXISTS solvcomps (
    samplecode varchar REFERENCES chm (samplecode),
    channel varchar NOT NULL,
    ch1_solv varchar NOT NULL,
    name_1 varchar,
    ch2_solv varchar,
    name_2 varchar,
    selected varchar NOT NULL,
    used varchar NOT NULL,
    percent float NOT NULL,
    PRIMARY KEY (samplecode, channel)
);

INSERT INTO solvcomps
SELECT
    chm.samplecode,
    sc.channel,
    sc.ch1_solv,
    sc.name_1,
    sc.ch2_solv,
    sc.name_2,
    sc.selected,
    sc.used,
    sc.percent
FROM
    solvcomp AS sc
INNER JOIN
    chm
    ON
        sc.id = chm.id
ORDER BY
    chm.samplecode;
