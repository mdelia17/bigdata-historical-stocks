CREATE TABLE prices (ticker STRING,
                     open DECIMAL(38,5),
                     close DECIMAL(38,5),
                     adj_close DECIMAL(38,5),
                     low DECIMAL(38,5),
                     high DECIMAL(38,5),
                     volume INTEGER,
                     ticker_date DATE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
LOCATION 's3://bucket-dataset-bigdata2/historical_stock_prices_clean/';

CREATE TABLE stocks (ticker STRING,
                     exc STRING,
                     name STRING,
                     sector STRING,
                     industry STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ','
LOCATION 's3://bucket-dataset-bigdata2/historical_stocks_clean/';

-- 1) per ogni settore e per ogni giorno la quotazione del settore (somma dei prezzi di chiusura delle azioni del settore)
CREATE VIEW t1 AS
SELECT ss.sector, ps.ticker_date AS day, SUM(ps.close) AS quotation
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ss.sector, ps.ticker_date;

-- 2) per ogni settore prima data utile per ogni anno, ultima data utile per ogni anno
CREATE VIEW t2 AS
SELECT ss.sector, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ss.sector, EXTRACT(YEAR FROM ps.ticker_date);

-- 3) join tra t1 e t2 per trovare la quotazione che corrisponde alle varie date (minime e massime di ogni anno) e poter fare la variazione (il join va fatto anche per settore, perché potrebbero esserci diversi settori che hanno le stesse date)
CREATE VIEW t3 AS
SELECT t1_1.sector, EXTRACT(YEAR FROM t2.min_date) AS yr, (t1_2.quotation - t1_1.quotation) / t1_1.quotation * 100 AS variation
FROM t2 JOIN t1 AS t1_1 ON t2.min_date = t1_1.day 
                        AND t2.sector = t1_1.sector
        JOIN t1 AS t1_2 ON t2.max_date = t1_2.day
                        AND t2.sector = t1_2.sector;

-- i primi 3 passaggi svolgono il punto (a)

-- 4) prima data utile dell'azione e ultima data utile dell'azione per ogni anno e il volume totale di ogni azione per ogni anno
CREATE VIEW t4 AS
SELECT ps.ticker, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date, SUM(ps.volume) AS sum_volume
FROM prices AS ps
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ps.ticker, EXTRACT(YEAR FROM ps.ticker_date);

-- 5) join per trovare i settori corrispondenti alle azioni (serve perché poi il nome dell'azione con variazione massima e volume massimo va presa secondo il settore)
CREATE VIEW t5 as
SELECT t4.ticker, t4.min_date, t4.max_date, ss.sector, t4.sum_volume
FROM t4 JOIN stocks AS ss ON t4.ticker = ss.ticker;
                
-- 6) join per ticker e date varie per prendere i valori corrispondenti alle date e calcolare l'incremento
CREATE VIEW t6 AS
SELECT t5.ticker, t5.sector, EXTRACT(YEAR FROM t5.min_date) AS yr, (ps2.close - ps1.close) / ps1.close * 100 AS variation, t5.sum_volume
FROM t5 JOIN prices AS ps1 ON t5.min_date = ps1.ticker_date 
                           AND t5.ticker = ps1.ticker
        JOIN prices AS ps2 ON t5.max_date = ps2.ticker_date 
                           AND t5.ticker = ps2.ticker;

-- 7) prendere la variazione massima e il volume massimo per ogni settore e per ogni anno 
CREATE VIEW t7 AS
SELECT t6.sector, t6.yr, MAX(t6.variation) AS max_variation, MAX(t6.sum_volume) as max_volume
FROM t6
GROUP BY t6.sector, t6.yr;

-- 8) join per prendere il nome dell'azione con variazione massima e il nome dell'azione con volume massimo
CREATE VIEW t8 AS
SELECT t7.sector, t7.yr, t6_1.ticker AS ticker_var, t7. max_variation, t6_2.ticker AS ticker_vol, t7.max_volume
FROM t7 JOIN t6 AS t6_1 ON t7.yr = t6_1.yr
				        AND t7.sector = t6_1.sector
				        AND t7.max_variation = t6_1.variation
		JOIN t6 AS t6_2 ON t7.yr = t6_2.yr
				        AND t7.sector = t6_2.sector
				        AND t7.max_volume = t6_2.sum_volume;

-- 9) join finale per unire i risultati del punto (a) con quelli dei punti (b) e (c)
CREATE TABLE t9 AS
SELECT t3.sector, t3.yr, t3.variation, t8.ticker_var, t8.max_variation, t8.ticker_vol, t8.max_volume
FROM t8 JOIN t3 ON t8.sector = t3.sector
                AND t8.yr = t3.yr
ORDER BY t3.sector;

DROP TABLE prices;
DROP TABLE stocks;
DROP VIEW t1;
DROP VIEW t2;
DROP VIEW t3;
DROP VIEW t4;
DROP VIEW t5;
DROP VIEW t6;
DROP VIEW t7;
DROP VIEW t8;