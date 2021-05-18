CREATE TABLE prices (ticker STRING,
                   open FLOAT,
                   close FLOAT,
                   adj_close FLOAT,
                   low FLOAT,
                   high FLOAT,
                   volume FLOAT,
                   ticker_date DATE)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

CREATE TABLE stocks (ticker STRING,
                   exc STRING,
                   name STRING,
                   sector STRING,
                   industry STRING)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH './Progetto/historical_stock_prices_clean.csv'
                        OVERWRITE INTO TABLE prices;

LOAD DATA LOCAL INPATH './Progetto/historical_stocks_clean.csv'
                        OVERWRITE INTO TABLE stocks;

-- 1) per ogni giorno la quotazione 
CREATE TABLE v1 AS
SELECT ss.sector, ps.ticker_date AS day, SUM(ps.close) AS quotation
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-01-01'
GROUP BY ss.sector, ps.ticker_date
ORDER BY ss.sector;

-- 2) prima data utile per ogni anno, ultima data utile per ogni anno, volume massimo per ogni anno
CREATE TABLE v2 AS
SELECT ss.sector, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date, MAX(ps.volume) AS max_volume
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-01-01'
GROUP BY ss.sector, EXTRACT(YEAR FROM ps.ticker_date)
ORDER BY ss.sector;

-- 3) join tra v1 e v2 per trovare la quotazione che corrisponde alle varie date e poter fare la variazione (il join va fatto anche per settore, perché potrebbero esserci diversi settori che hanno le stesse date)
CREATE TABLE v3 AS
SELECT v1_1.sector, EXTRACT(YEAR FROM v2.min_date) AS yr, (v1_2.quotation - v1_1.quotation) / v1_1.quotation * 100 AS variation, v2.max_volume
FROM v2 JOIN v1 AS v1_1 ON v2.min_date = v1_1.day 
                        AND v2.sector = v1_1.sector
        JOIN v1 AS v1_2 ON v2.max_date = v1_2.day
                        AND v2.sector = v1_2.sector
ORDER BY v1_1.sector;

-- 4) join tra v3 e prices per prendere il nome dell'azione corrispondente al volume massimo (ATTENZIONE CI SONO RIPETIZIONI, VEDI COME SISTEMARE)
CREATE TABLE v4 AS
SELECT v3.sector, v3.yr, v3.variation, v3.max_volume, ps.ticker
FROM v3 JOIN prices AS ps ON v3.max_volume = ps.volume
ORDER BY v3.sector;

-- i primi 4 passaggi svolgono i punti a e c dell'esercizio 2 

-- 5) prima data utile dell'azione e ultima data utile dell'azione per ogni anno
CREATE TABLE v5 AS
SELECT ps.ticker, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date
FROM prices AS ps
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-01-01'
GROUP BY ps.ticker, EXTRACT(YEAR FROM ps.ticker_date);

-- 6) join per trovare i settori corrispondenti alle azioni
CREATE TABLE v6 as
SELECT v5.ticker, v5.min_date, v5.max_date, ss.sector
FROM v5 JOIN stocks AS ss ON v5.ticker = ss.ticker;
                
-- 7) join per ticker e date varie per prendere i valori corrispondenti alle date e calcolare l'incremento
CREATE TABLE v7 AS
SELECT v6.ticker, v6.sector, EXTRACT(YEAR FROM v6.min_date) AS yr, (ps2.close - ps1.close) / ps1.close * 100 AS variation
FROM v6 JOIN prices AS ps1 ON v6.min_date = ps1.ticker_date 
                        AND v6.ticker = ps1.ticker
        JOIN prices AS ps2 ON v6.max_date = ps2.ticker_date 
                        AND v6.ticker = ps2.ticker
ORDER BY v6.sector, yr;

-- 8) prendere la variazione massima per ogni settore e per ogni anno
CREATE TABLE v8 AS
SELECT v7.sector, v7.yr, MAX(v7.variation) AS max_variation
FROM v7
GROUP BY v7.sector, v7.yr
ORDER BY v7.sector, v7.yr;

-- 9) join per prendere il nome dell'azione con variazione massima
CREATE TABLE v9 AS
SELECT v8.sector, v8.yr, v7.ticker, v8. max_variation
FROM v8 JOIN v7 ON v8.yr = v7.yr
				AND v8.sector = v7.sector
				AND v8.max_variation = v7.variation
ORDER BY v8.sector, v8.yr;

-- 10) join finale
SELECT v4.sector, v4.yr, v4.variation, v9.ticker, v9.max_variation, v4.ticker, v4.max_volume
FROM v9 JOIN v4 ON v9.sector = v4.sector
                AND v9.yr = v4.yr
ORDER BY v4.sector, v4.yr;

DROP TABLE prices;
DROP TABLE stocks;
DROP TABLE v1;
DROP TABLE v2;
DROP TABLE v3;
DROP TABLE v4;
DROP TABLE v5;
DROP TABLE v6;
DROP TABLE v7;
DROP TABLE v8;
DROP TABLE v9;