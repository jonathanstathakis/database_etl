CREATE TABLE if not exists solvcomps (
    pk INTEGER REFERENCES chm (pk),
    channel VARCHAR NOT NULL,
    ch1_solv VARCHAR NOT NULL,
    name_1 VARCHAR,
    ch2_solv VARCHAR,
    name_2 VARCHAR,
    selected VARCHAR NOT NULL,
    used VARCHAR NOT NULL,
    percent FLOAT NOT NULL,
    PRIMARY KEY (pk, channel)
);

INSERT INTO solvcomps
SELECT
    chm.pk,
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
    chm.pk;
