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

-- 1) per ogni azione prendo il primo giorno e l'ultimo giorno in cui compare per ogni mese del 2017
CREATE VIEW t1 AS
SELECT ps.ticker, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date
FROM prices AS ps
WHERE ps.ticker_date BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY ps.ticker, EXTRACT(MONTH FROM ps.ticker_date);

-- 2) join per prendere per ogni data il prezzo di chiusura per fare la variazione per ogni azioni e per ogni mese
CREATE VIEW t2 AS
SELECT t1.ticker, EXTRACT(MONTH FROM t1.min_date) AS month, (ps2.close - ps1.close) / ps1.close * 100 AS variation
FROM t1 JOIN prices AS ps1 ON t1.min_date = ps1.ticker_date
						   AND t1.ticker = ps1.ticker
		JOIN prices AS ps2 ON t1.max_date = ps2.ticker_date
						   AND t1.ticker = ps2.ticker;

-- 3) join per prendere il nome dell'azienda e raggruppamento per ogni azienda e per ogni mese per trovare la variazione mensile
CREATE VIEW t3 AS
SELECT ss.name, month, AVG(t2.variation) AS variation
FROM stocks AS ss JOIN t2 ON ss.ticker = t2.ticker
GROUP BY ss.name, month;

-- 4) per ogni azienda trovo il numero di mesi dove l'azienda ha informazioni
CREATE VIEW t4 AS
SELECT t3.name, COUNT(month) AS count_month
FROM t3
GROUP BY t3.name;

-- 5) faccio il join con t3 per mettere tutte le informazioni insieme 
CREATE VIEW t5 AS 
SELECT t3.name, month, t3.variation, t4.count_month
FROM t3 JOIN t4 ON t3.name = t4.name;

-- 6) prodotto cartesiano per poter confrontare le aziende che hanno lo stesso numero di mesi (e questo è maggiore di 6), gli stessi mesi, nome diverso e la differenza tra le variazioni delle aziende deve essere < 1, mi segno anche il numero di mesi nei quali le aziende dovrebbero essere simili (che sarebbe il numero di mesi nei quali le aziende hanno informazioni)
CREATE VIEW t6 AS
SELECT t5_1.name AS a1, t5_2.name AS a2, t5_1.month, ABS(t5_1.variation - t5_2.variation) AS sim, t5_1.count_month
FROM t5 AS t5_1 CROSS JOIN t5 AS t5_2 
WHERE t5_1.name != t5_2.name AND t5_1.count_month = t5_2.count_month
					  		 AND t5_1.count_month > 6
							 AND t5_1.month = t5_2.month 
							 AND ABS(t5_1.variation - t5_2.variation) < 1;

-- 7) controllo quanti sono i mesi nei quali le coppie di aziende sono simili che sono quelli dove la differenza delle variazioni è < 1 
CREATE VIEW t7 AS
SELECT t6.a1, t6.a2, COUNT(t6.month) AS mesi_sim
FROM t6
GROUP BY t6.a1, t6.a2;

-- 8) prendo solo le aziende che sono simili in un numero di mesi che è pari effettivamente al numero di mesi dove le aziende hanno informazioni
CREATE VIEW t8 AS
SELECT t6.a1, t6.a2, t6.month
FROM t6 JOIN t7 ON t6.a1 = t7.a1
				AND t6.a2 = t7.a2
WHERE t7.mesi_sim = t6.count_month;

-- 9) stampa finale
CREATE TABLE t9 AS
SELECT CONCAT('(', t8.a1, ',', t8.a2, '):') AS a1_a2, t8.month, t8.a1, t3_1.variation AS v_a1, t8.a2, t3_2.variation AS v_a2
FROM t8 JOIN t3 AS t3_1 ON t8.a1 = t3_1.name
						AND t8.month = t3_1.month
		JOIN t3 AS t3_2 ON t8.a2 = t3_2.name
						AND t8.month = t3_2.month
ORDER BY a1_a2;

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