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

-- 1) si filtra per l'intervallo di giorni richiesto dall'esercizio, si raggruppa per ogni settore e per ogni giorno e si calcola la quotazione del settore per un certo giorno (somma dei prezzi di chiusura delle azioni del settore)
CREATE VIEW v1 AS
SELECT ss.sector, ps.ticker_date AS day, SUM(ps.close) AS quotation
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ss.sector, ps.ticker_date;

-- 2) si fa il join tra la tabella stocks e prices su ticker, si raggruppa per settore e per gli anni dei ticker in modo da prendere, per ogni settore, la prima data utile per ogni anno, ultima data utile per ogni anno
CREATE VIEW v2 AS
SELECT ss.sector, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date
FROM stocks AS ss JOIN prices AS ps ON ss.ticker = ps.ticker
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ss.sector, EXTRACT(YEAR FROM ps.ticker_date);

-- 3) si fa il join tra v1 e v2 su la data minima e il settore e su la data massima e il settore per trovare la quotazione che corrisponde alle varie date (minime e massime di ogni anno) e poter fare la variazione (il join va fatto anche per settore, perché potrebbero esserci diversi settori che hanno le stesse date)
CREATE VIEW v3 AS
SELECT v1_1.sector, EXTRACT(YEAR FROM v2.min_date) AS yr, (v1_2.quotation - v1_1.quotation) / v1_1.quotation * 100 AS variation
FROM v2 JOIN v1 AS v1_1 ON v2.min_date = v1_1.day 
                        AND v2.sector = v1_1.sector
        JOIN v1 AS v1_2 ON v2.max_date = v1_2.day
                        AND v2.sector = v1_2.sector;

-- i primi 3 passaggi svolgono il punto (a)

-- 4) si raggruppa per ticker e per ogni anno, in modo da trovare la prima data utile di ogni azione e ultima data utile di ogni azione per ogni anno e il volume totale di ogni azione per ogni anno
CREATE VIEW v4 AS
SELECT ps.ticker, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date, SUM(ps.volume) AS sum_volume
FROM prices AS ps
WHERE ps.ticker_date BETWEEN '2009-01-01' AND '2018-12-31'
GROUP BY ps.ticker, EXTRACT(YEAR FROM ps.ticker_date);

-- 5) si fa un join tra il risultato precedente e stocks su ticker per trovare i settori corrispondenti alle azioni (serve perché in seguito il nome dell'azione con variazione massima e volume massimo va presa in base al settore)
CREATE VIEW v5 as
SELECT v4.ticker, v4.min_date, v4.max_date, ss.sector, v4.sum_volume
FROM v4 JOIN stocks AS ss ON v4.ticker = ss.ticker;
                
-- 6) si fa un join tra il risultato precedente e prices su ticker e date varie per prendere i valori corrispondenti alle date e calcolare l'incremento
CREATE VIEW v6 AS
SELECT v5.ticker, v5.sector, EXTRACT(YEAR FROM v5.min_date) AS yr, (ps2.close - ps1.close) / ps1.close * 100 AS variation, v5.sum_volume
FROM v5 JOIN prices AS ps1 ON v5.min_date = ps1.ticker_date 
                           AND v5.ticker = ps1.ticker
        JOIN prices AS ps2 ON v5.max_date = ps2.ticker_date 
                           AND v5.ticker = ps2.ticker;

-- 7) si raggruppa per settore e per anno in modo da prendere la variazione massima e il volume massimo per ogni settore e per ogni anno 
CREATE VIEW v7 AS
SELECT v6.sector, v6.yr, MAX(v6.variation) AS max_variation, MAX(v6.sum_volume) as max_volume
FROM v6
GROUP BY v6.sector, v6.yr;

-- 8) si fanno vari join per prendere il nome dell'azione con variazione massima e il nome dell'azione con volume massimo per ogni settore
CREATE VIEW v8 AS
SELECT v7.sector, v7.yr, v6_1.ticker AS ticker_var, v7. max_variation, v6_2.ticker AS ticker_vol, v7.max_volume
FROM v7 JOIN v6 AS v6_1 ON v7.yr = v6_1.yr
				        AND v7.sector = v6_1.sector
				        AND v7.max_variation = v6_1.variation
		JOIN v6 AS v6_2 ON v7.yr = v6_2.yr
				        AND v7.sector = v6_2.sector
				        AND v7.max_volume = v6_2.sum_volume;

-- 9) join finale per unire i risultati del punto (a) con quelli dei punti (b) e (c) e ordinamento per settore
CREATE TABLE t9 AS
SELECT v3.sector, v3.yr, v3.variation, v8.ticker_var, v8.max_variation, v8.ticker_vol, v8.max_volume
FROM v8 JOIN v3 ON v8.sector = v3.sector
                AND v8.yr = v3.yr
ORDER BY v3.sector;

DROP TABLE prices;
DROP TABLE stocks;
DROP VIEW v1;
DROP VIEW v2;
DROP VIEW v3;
DROP VIEW v4;
DROP VIEW v5;
DROP VIEW v6;
DROP VIEW v7;
DROP VIEW v8;