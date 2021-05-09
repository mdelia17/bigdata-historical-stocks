CREATE TABLE docs (ticker STRING,
                   open FLOAT,
                   close FLOAT,
                   adj_close FLOAT,
                   low FLOAT,
                   high FLOAT,
                   volume FLOAT,
                   ticker_date DATE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

-- non si può usare date come nome di una colonna perché è riservato

LOAD DATA LOCAL INPATH './Progetto/historical_stock_prices.csv'
                        OVERWRITE INTO TABLE docs;

SELECT * FROM docs LIMIT 10;
 
CREATE TABLE group_prices1 AS
    SELECT p1.ticker AS ticker, MIN(p1.ticker_date) AS min_date, MAX(p1.ticker_date) AS max_date, MIN(low) AS min_low, MAX(high) as max_high
    FROM docs AS p1 
    GROUP BY p1.ticker;

-- SELECT * FROM group_prices1;

CREATE TABLE group_prices2 AS
    SELECT gp.ticker, gp.min_date, gp.max_date, gp.min_low, gp.max_high, p2.close AS first_close
    FROM group_prices1 AS gp JOIN docs AS p2 ON gp.ticker = p2.ticker
                            AND gp.min_date = p2.ticker_date;

-- si può fare anche il join che sta sotto qua sopra, per compattare, e di conseguenza anche l'ultima operazione

-- SELECT * FROM group_prices2;

CREATE TABLE group_prices3 as
    SELECT gp.ticker, gp.min_date, gp.max_date, gp.min_low, gp.max_high, gp.first_close, p3.close AS last_close
    FROM group_prices2 AS gp JOIN docs AS p3 ON gp.ticker = p3.ticker
                            AND gp.max_date = p3.ticker_date;

-- SELECT * FROM group_prices3;

SELECT gp.ticker, gp.min_date, gp.max_date, gp.min_low, gp.max_high, (gp.last_close - gp.first_close) / gp.first_close * 100 AS variation
FROM group_prices3 AS gp
ORDER BY gp.max_date;

DROP TABLE docs;
DROP TABLE group_prices1;
DROP TABLE group_prices2;
DROP TABLE group_prices3;