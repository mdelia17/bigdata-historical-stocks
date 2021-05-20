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

-- 1) raggruppare per ticker e per ogni ticker selezionare la data più piccola (a), la data più grande (b), il prezzo massimo (il massimo della colonna high) e il prezzo minimo (il minimo della colonna low) (d)  
CREATE VIEW group_prices1 AS
SELECT p1.ticker AS ticker, MIN(p1.ticker_date) AS min_date, MAX(p1.ticker_date) AS max_date, MAX(high) AS max_high, MIN(low) AS min_low
FROM prices AS p1 
GROUP BY p1.ticker;

-- 2) fare il join su ticker e min_date per trovare il primo prezzo di chiusura, fare il join su ticker e max_date per trovare l'ultimo prezzo di chiusura, calcolare la variazione percentuale (c)
CREATE VIEW group_prices2 AS
SELECT gp.ticker, gp.min_date, gp.max_date, (p3.close - p2.close) / p2.close * 100 AS variation, gp.max_high, gp.min_low 
FROM group_prices1 AS gp JOIN prices AS p2 ON gp.ticker = p2.ticker
                                        AND gp.min_date = p2.ticker_date
                        JOIN prices AS p3 ON gp.ticker = p3.ticker
                                        AND gp.max_date = p3.ticker_date
ORDER BY gp.max_date DESC, gp.ticker;

-- i passaggi sotto svolgono il punto (d)

-- 3) per differenziare valori positivi e negativi nella tabella prices bisogna usare SIGN (restituisce 1 per i postitivi, -1 per i negati, 0 per gli uguali) 
CREATE VIEW t1 AS
SELECT ps.ticker, SIGN(ps.close - ps.open) AS ticker_up, ps.ticker_date
FROM prices AS ps;

CREATE VIEW t2 AS
SELECT t1.ticker, t1.ticker_up, t1.ticker_date, t1.ticker_date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY t1. ticker, t1.ticker_up ORDER BY t1.ticker_date)) DAY AS grp
FROM t1;

CREATE VIEW t3 AS
SELECT t2.ticker, t2.ticker_up, COUNT(*) AS seq, MIN(t2.ticker_date) AS min_dt, MAX(t2.ticker_date) AS max_dt
FROM t2
GROUP BY t2.ticker, t2.ticker_up, t2.grp;

CREATE VIEW t4 AS
SELECT t3.ticker, t3.ticker_up,
CASE WHEN (t3.ticker_up = -1) THEN 0
WHEN (t3.ticker_up = 0) THEN 0
ELSE t3.seq
END AS seq, t3.min_dt, t3.max_dt
FROM t3;
    
-- 4) così trovo per ogni ticker la sequenza massima
CREATE VIEW t5 AS
SELECT t4.ticker, MAX(t4.seq) AS max_seq
FROM t4
GROUP BY t4.ticker;

-- 5) parte più interna della query precedente, sarebbe t3

-- 6) questo è la cosa migliore, altrimenti se in un anno ci più sequenze massime uguali le mette tutte, però se in diversi anni la streak massima è la stessa? Di quale anno si prende? Prendiamo l'ultimo anno
CREATE VIEW t6 AS
SELECT t5.ticker, MAX(t5.max_seq) AS max_seq, MAX(EXTRACT(YEAR FROM t4.max_dt)) AS year
FROM t5 JOIN t4 ON t5.ticker = t4.ticker
				AND t5.max_seq = t4.seq
GROUP BY t5.ticker; 

CREATE TABLE t7 AS
SELECT gp.ticker, gp.min_date, gp.max_date, gp.variation, gp.max_high, gp.min_low, t6.max_seq, t6.year
FROM group_prices2 AS gp JOIN t6 ON gp.ticker = t6.ticker;

DROP TABLE prices;
DROP VIEW group_prices1;
DROP VIEW group_prices2;
DROP VIEW t1;
DROP VIEW t2;
DROP VIEW t3;
DROP VIEW t4;
DROP VIEW t5;
DROP VIEW t6;