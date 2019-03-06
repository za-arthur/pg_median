CREATE TABLE intvals(val int, color text);

-- Test empty table
SELECT median(val) FROM intvals;

-- Integers with odd number of values
INSERT INTO intvals VALUES
       (1, 'a'),
       (2, 'c'),
       (9, 'b'),
       (7, 'c'),
       (2, 'd'),
       (-3, 'd'),
       (2, 'e');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers with NULLs and even number of values
INSERT INTO intvals VALUES
       (99, 'a'),
       (NULL, 'a'),
       (NULL, 'e'),
       (NULL, 'b'),
       (7, 'c'),
       (0, 'd');

SELECT * FROM intvals ORDER BY val;
SELECT median(val) FROM intvals;

-- Integers GROUP BY color
SELECT color, median(val) FROM intvals GROUP BY color ORDER BY color;

-- Window function with integers
SELECT color, val, median(val) OVER (PARTITION BY color) FROM intvals ORDER BY color, val;

-- Text values
CREATE TABLE textvals(val text, color int);

INSERT INTO textvals VALUES
       ('erik', 1),
       ('mat', 3),
       ('rob', 8),
       ('david', 9),
       ('lee', 2);

SELECT * FROM textvals ORDER BY val;
SELECT median(val) FROM textvals;

-- Text with even number of values
INSERT INTO textvals VALUES
       ('extra', 5);

SELECT median(val) FROM textvals; -- fails

-- Test large table with timestamps
CREATE TABLE timestampvals (val timestamptz);

INSERT INTO timestampvals(val)
SELECT TIMESTAMP 'epoch' + (i * INTERVAL '1 second')
FROM generate_series(0, 100000) as T(i);

SELECT median(val) FROM timestampvals;

-- Force use of parallelism
ALTER TABLE timestampvals set (parallel_workers = 4);
SET parallel_setup_cost = 0;
SET max_parallel_workers_per_gather = 4;

EXPLAIN (COSTS OFF) SELECT median(val) FROM timestampvals;
SELECT median(val) FROM timestampvals;
