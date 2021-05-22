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

-- 1) si filtra per l'intervallo di giorni richiesto dall'esercizio, si raggruppa per azione e per mese in modo da trovare, per ogni azione, il primo giorno e l'ultimo giorno in cui questa compare per ogni mese del 2017
CREATE VIEW v1 AS
SELECT ps.ticker, MIN(ps.ticker_date) AS min_date, MAX(ps.ticker_date) AS max_date
FROM prices AS ps
WHERE ps.ticker_date BETWEEN '2017-01-01' AND '2017-12-31'
GROUP BY ps.ticker, EXTRACT(MONTH FROM ps.ticker_date);

-- 2) si fanno vari join per prendere i prezzi di chiusura corrispondenti alle varie date in modo da trovare la variazione per ogni azione e per ogni mese
CREATE VIEW v2 AS
SELECT v1.ticker, EXTRACT(MONTH FROM v1.min_date) AS month, (ps2.close - ps1.close) / ps1.close * 100 AS variation
FROM v1 JOIN prices AS ps1 ON v1.min_date = ps1.ticker_date
						   AND v1.ticker = ps1.ticker
		JOIN prices AS ps2 ON v1.max_date = ps2.ticker_date
						   AND v1.ticker = ps2.ticker;

-- 3) si fa un join per prendere il nome dell'azienda corrispondente all'azione e si raggruppa per ogni azienda e per ogni mese per trovare la variazione mensile
CREATE VIEW v3 AS
SELECT ss.name, month, AVG(v2.variation) AS variation
FROM stocks AS ss JOIN v2 ON ss.ticker = v2.ticker
GROUP BY ss.name, month;

-- 4) per ogni azienda si trova il numero di mesi dove l'azienda ha informazioni
CREATE VIEW v4 AS
SELECT v3.name, COUNT(month) AS count_month
FROM v3
GROUP BY v3.name;

-- 5) si fa il join per mettere tutte le informazioni insieme 
CREATE VIEW v5 AS 
SELECT v3.name, month, v3.variation, v4.count_month
FROM v3 JOIN v4 ON v3.name = v4.name;

-- 6) si fa un prodotto cartesiano per poter confrontare le aziende che hanno lo stesso numero di mesi (e questo deve essere maggiore o uguale a 6), gli stessi mesi, nome diverso e la differenza tra le variazioni delle aziende deve essere <= 1 (soglia), si tiene traccia anche del numero di mesi nei quali le aziende dovrebbero essere simili (che sarebbe il numero di mesi nei quali le aziende hanno informazioni)
CREATE VIEW v6 AS
SELECT v5_1.name AS a1, v5_2.name AS a2, v5_1.month, ABS(v5_1.variation - v5_2.variation) AS sim, v5_1.count_month
FROM v5 AS v5_1 CROSS JOIN v5 AS v5_2 
WHERE v5_1.name != v5_2.name AND v5_1.count_month = v5_2.count_month
					  		 AND v5_1.count_month >= 6
							 AND v5_1.month = v5_2.month 
							 AND ABS(v5_1.variation - v5_2.variation) <= 1;

-- 7) si controlla quanti sono i mesi nei quali le coppie di aziende sono simili che sono quelli dove la differenza delle variazioni è < 1 
CREATE VIEW v7 AS
SELECT v6.a1, v6.a2, COUNT(v6.month) AS mesi_sim
FROM v6
GROUP BY v6.a1, v6.a2;

-- 8) si prendono solo le aziende che sono simili in un numero di mesi che è pari effettivamente al numero di mesi dove le aziende hanno informazioni (se un'azienda A ha informazioni su 2 mesi e un'azienda B ha informazioni su 12 mesi e queste sono simili sue 2 mesi non devono essere comunque considerate simili in quanto si hanno troppe poche informazioni per dirlo)
CREATE VIEW v8 AS
SELECT v6.a1, v6.a2, v6.month
FROM v6 JOIN v7 ON v6.a1 = v7.a1
				AND v6.a2 = v7.a2
WHERE v7.mesi_sim = v6.count_month;

-- 9) stampa finale: utilizza la funzione CONCAT per rendere più leggibile l'output e si ordina per lo stesso motivo, anche se si perde di efficienza 
CREATE TABLE t9 AS
SELECT CONCAT('(', v8.a1, ',', v8.a2, '):') AS a1_a2, v8.month, v8.a1, v3_1.variation AS v_a1, v8.a2, v3_2.variation AS v_a2
FROM v8 JOIN v3 AS v3_1 ON v8.a1 = v3_1.name
						AND v8.month = v3_1.month
		JOIN v3 AS v3_2 ON v8.a2 = v3_2.name
						AND v8.month = v3_2.month
ORDER BY a1_a2;

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