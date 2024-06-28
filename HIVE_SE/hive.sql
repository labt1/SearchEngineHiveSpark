DROP TABLE IF EXISTS pages;
DROP TABLE IF EXISTS inverted_index;
DROP TABLE IF EXISTS pagerank;

CREATE EXTERNAL TABLE pages (
    page_id INT,
    content STRING
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://labt-bg01/input_ii/';

CREATE TABLE inverted_index (
    word STRING,
    page_id INT
);

INSERT OVERWRITE TABLE inverted_index
SELECT word, page_id
FROM pages
LATERAL VIEW explode(split(content, ' ')) exploded_table AS word;


CREATE TABLE pagerank (
    page_id INT,
    rank DOUBLE
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
LOCATION 's3://labt-bg01/input_pr/';

INSERT OVERWRITE TABLE pagerank
SELECT t1.page_id, 0.15 + 0.85 * SUM(t2.rank)
FROM pages t1
JOIN pagerank t2 ON t1.page_id = t2.page_id
GROUP BY t1.page_id;

--------------------------------------------------
-----Consulta
SELECT ii.page_id, pr.rank
FROM inverted_index ii
JOIN pagerank pr ON ii.page_id = pr.page_id
WHERE ii.word LIKE 'a'
ORDER BY pr.rank DESC;

SELECT DISTINCT ii.page_id, pr.rank FROM inverted_index ii JOIN pagerank pr ON ii.page_id = pr.page_id WHERE ii.word LIKE 'Hive' ORDER BY pr.rank DESC