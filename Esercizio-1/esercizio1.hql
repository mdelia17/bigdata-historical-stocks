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

-- 1) si raggruppa per ticker e per ogni ticker si seleziona la data più piccola (a), la data più grande (b), il prezzo massimo (il massimo della colonna high) e il prezzo minimo (il minimo della colonna low) (d)  
CREATE VIEW v1 AS
SELECT p1.ticker AS ticker, MIN(p1.ticker_date) AS min_date, MAX(p1.ticker_date) AS max_date, MAX(high) AS max_high, MIN(low) AS min_low
FROM prices AS p1 
GROUP BY p1.ticker;

-- 2) si fa il join tra la tabella prices e il risultato precedente su ticker e min_date per trovare il primo prezzo di chiusura, si fa il join tra la tabella prices e il risultato precedente su ticker e max_date per trovare l'ultimo prezzo di chiusura, si calcola la variazione percentuale (c)
CREATE VIEW v2 AS
SELECT v1.ticker, v1.min_date, v1.max_date, (p3.close - p2.close) / p2.close * 100 AS variation, v1.max_high, v1.min_low 
FROM v1 JOIN prices AS p2 ON v1.ticker = p2.ticker
                          AND v1.min_date = p2.ticker_date
        JOIN prices AS p3 ON v1.ticker = p3.ticker
                          AND v1.max_date = p3.ticker_date;

-- i passaggi sotto svolgono il punto (d)

-- 3) per differenziare valori positivi e negativi nella tabella prices bisogna usare SIGN (restituisce 1 per i positivi, -1 per i negati, 0 per gli uguali) 
CREATE VIEW v3 AS
SELECT ps.ticker, SIGN(ps.close - ps.open) AS ticker_up, ps.ticker_date
FROM prices AS ps;

-- 4) si utilizza una window function o (analytical function) che permette di attribuire ad ogni riga la posizione che questa avrebbe se si raggruppasse per ticker e per risultato della differenza tra close e open e se si ordinasse per data, questo consente di individuare la streak. Ad esempio se si ha che per l'azione AHH il 2015-05-04 si ha una differenza tra close e open positiva (indicata dal valore 1), stessa cosa per il giorno 2015-05-05 e 2015-05-06, se si raggruppa per ticker e 1 e si ordina per data, si avrebbe che alla riga AHH 1 2015-05-04 viene affiancato il valore 1 (numero della riga dovuta a raggruppamento e ordinamento), alla riga AHH 1 2015-05-05 viene affiancato il valore 2 e alla riga AHH 1 2015-05-06 viene affiancato il valore 3. Se poi si sottrae alla data iniziale il valore della colonna affiancata si ha che quelle tre righe ottengono la stessa data (2015-05-04 - 1 giorno = 2015-05-03; 2015-05-05 - 2 giorni = 2015-05-03; 2015-05-06 - 3 giorni = 2015-05-03). Da notare che la funzione INTERVAL tiene conto del fatto che se si sottraggono 3 giorni al 2015-06-1 si ottiene 2015-05-29 
CREATE VIEW v4 AS
SELECT v3.ticker, v3.ticker_up, v3.ticker_date, v3.ticker_date - INTERVAL (ROW_NUMBER() OVER (PARTITION BY v3. ticker, v3.ticker_up ORDER BY v3.ticker_date)) DAY AS grp
FROM v3;

-- 5) raggruppando per la colonna aggiunta prima e contando quante righe contiene quel raggruppamento si ottiene il numero che corrisponde alla streak. Inoltre prendendo la minima data e la massima data del raggruppamento si ha anche da quando parte e da quando finisce la streak. Da notare che solo i giorni consecutivi appartengono ad una stessa streak, infatti se si hanno due date 2015-05-04 e 2015-05-06 e non si ha 2015-05-05, quando si raggruppa e si attribuisce ad ogni data la riga che questa avrebbe nel raggruppamento e poi si sottrae il valore di questa riga alla data si ottiene che alla data 2015-05-04 si affianca 2015-05-03 e alla data 2015-05-06 si affianca 2015-05-04, quindi queste due date non appartengono alla stessa streak
CREATE VIEW v5 AS
SELECT v4.ticker, v4.ticker_up, COUNT(*) AS seq, MIN(v4.ticker_date) AS min_dt, MAX(v4.ticker_date) AS max_dt
FROM v4
GROUP BY v4.ticker, v4.ticker_up, v4.grp;

-- 6) si sostituiscono le streak che corrispondono a valori 0 e -1, in quanto potrebbero influenzare sul calcolo della massima streak "positiva" (close - open > 0)
CREATE VIEW v6 AS
SELECT v5.ticker, v5.ticker_up,
CASE WHEN (v5.ticker_up = -1) THEN 0
WHEN (v5.ticker_up = 0) THEN 0
ELSE v5.seq
END AS seq, v5.min_dt, v5.max_dt
FROM v5;
    
-- 7) si trova per ogni ticker la sequenza massima
CREATE VIEW v7 AS
SELECT v6.ticker, MAX(v6.seq) AS max_seq
FROM v6
GROUP BY v6.ticker;

-- 8) si fa un join tra il risultato precedente e il risultato della vista v6 su ticker e sequenza massima per trovare la data che corrisponde alla sequenza massima, si fa poi un raggruppamento su ticker in modo da prendere solo una streak massima, quella relativa all'ultimo anno
CREATE VIEW v8 AS
SELECT v7.ticker, MAX(v7.max_seq) AS max_seq, MAX(EXTRACT(YEAR FROM v6.max_dt)) AS year
FROM v7 JOIN v6 ON v7.ticker = v6.ticker
				AND v7.max_seq = v6.seq
GROUP BY v7.ticker; 

-- 9) si fa un join tra il risultato precedente il risultato della vista v2 su ticker in modo da mettere insieme tutti i risultati e ordinare per la data massima in ordine decrescente
CREATE TABLE t7 AS
SELECT v2.ticker, v2.min_date, v2.max_date, v2.variation, v2.max_high, v2.min_low, v8.max_seq, v8.year
FROM v2 JOIN v8 ON v2.ticker = v8.ticker
ORDER BY v2.max_date DESC, v2.ticker;

DROP TABLE prices;
DROP VIEW v1;
DROP VIEW v2;
DROP VIEW v3;
DROP VIEW v4;
DROP VIEW v5;
DROP VIEW v6;
DROP VIEW v7;
DROP VIEW v8;