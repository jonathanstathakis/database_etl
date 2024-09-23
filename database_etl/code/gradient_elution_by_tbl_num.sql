-- get the zero time percentages of each solvent

CREATE OR REPLACE TEMP TABLE
zero_percents AS
SELECT
    num,
    0 AS time,
    percent,
    lower(channel) AS channel
FROM
    bin_pump.solvcomps;

-- melt the timetables table so the channels are a label column for later pivoting
CREATE OR REPLACE TEMP TABLE unpiv_tt AS
UNPIVOT
(
    SELECT
        num,
        time,
        a,
        b
    FROM
        bin_pump.timetables
)
ON
a, b
INTO
NAME
channel
VALUE
percent
ORDER BY
    num;

-- stack the zero time tables and the melted timetable so all time values are in one table
CREATE OR REPLACE TEMP TABLE all_percents AS (
    SELECT *
    FROM
        zero_percents
    UNION
    SELECT *
    FROM
        unpiv_tt
    ORDER BY
        num,
        time,
        channel
);

-- pivot back to wide
CREATE OR REPLACE TEMP TABLE all_perc_piv AS (
    SELECT
        *
    FROM (
    PIVOT
    all_percents
    ON
    channel
    USING
    first(percent)
    ORDER BY
        num,
        time
    )
)

SELECT * FROM all_perc_piv;
-- CREATE OR REPLACE TEMP TABLE all_perc_piv AS
-- SELECT
--     num,
--     time,
--     a,
--     b,
--     dense_rank() OVER (PARTITION BY num ORDER BY time) AS entry
-- FROM
--     all_perc_piv
-- ORDER BY
--     num,
--     entry,
--     time;
-- SELECT * FROM all_perc_piv
/*
CREATE OR RE trPLACE TEMP TABLE gradient_calc AS (
SELECT
    num,
    entry,
    time,
    b,
    lead(b) OVER (PARTITION by num ORDER BY entry) - b as diff_b,
    lead(time) OVER (PARTITION by num ORDER BY entry) - time as diff_time,
    diff_b / diff_time as gradient,
FROM
    all_perc_piv
ORDER BY
    num,
    entry
);

CREATE OR REPLACE TEMP TABLE gradient_per_sample AS (
SELECT
    num,
    first(gradient) as gradient
FROM
    gradient_calc
GROUP BY
    num
ORDER BY
    num
);

SELECT
    *
FROM
    gradient_per_sample;
*/
