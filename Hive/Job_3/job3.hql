DROP TABLE historical_stock_prices; 
DROP TABLE sectors; 
DROP TABLE azioniAzienda;
DROP TABLE firstAndLastMese;
DROP TABLE firstCLose;
DROP TABLE lastCLose;
DROP TABLE variazionePercentuale;


CREATE TABLE historical_stock_prices (ticker STRING, open float, close float, adj_close float,lowThe float, highThe float, volume float, dates date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
				OVERWRITE INTO TABLE historical_stock_prices;


CREATE TABLE sectors (ticker STRING, exchage STRING, name STRING, sector STRING,industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
				OVERWRITE INTO TABLE sectors;

--seleziono l'anno 2017 e per ogni azienda, prendo le azioni in quell'anno 
CREATE TABLE azioniAzienda AS 
SELECT hsp.ticker AS ticker, hsp.close AS close, hsp.dates, sectors.name AS name
FROM historical_stock_prices AS hsp JOIN sectors ON hsp.ticker=sectors.ticker
WHERE YEAR(hsp.dates) = '2017';

--per ogni mese prendo la prima e l'ultima data di chiusura dell'azione  
--ZYNE	2017-10-02	2017-10-31
--ZYNE	2017-11-01	2017-11-30
--ZYNE	2017-12-01	2017-12-29
CREATE TABLE firstAndLastMese AS
SELECT ticker, min((dates)) AS min_data , max((dates)) AS max_data 
FROM azioniAzienda
GROUP BY ticker,MONTH(dates); 

--per ogni mese prendo il primo  prezzo di chiusura dell'azione
--NZF	8	15.26	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	9	15.34	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	10	15.22	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	11	15.01	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	12	15.24	NUVEEN MUNICIPAL CREDIT INCOME FUND
CREATE TABLE firstCLose AS
SELECT b.ticker,MONTH(b.min_data) AS mese ,a.close AS close ,a.name
FROM azioniAzienda AS a , firstAndLastMese AS b
WHERE a.dates=b.min_data AND a.ticker=b.ticker; 

--per ogni mese prendo l'ultimo prezzo di chiusura dell'azione
--NZF	8	15.3	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	9	15.21	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	10	15.01	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	11	15.18	NUVEEN MUNICIPAL CREDIT INCOME FUND
--NZF	12	15.24	NUVEEN MUNICIPAL CREDIT INCOME FUND
CREATE TABLE lastCLose AS
SELECT b.ticker,MONTH(b.max_data) AS mese,a.close AS close, a.name
FROM azioniAzienda AS a , firstAndLastMese AS b
WHERE a.dates=b.max_data AND a.ticker=b.ticker; 

--per ogni mese calcolo la variazione percentuale 
--AKRX	1	-13.457178769472328	AKORN, INC.
--AKRX	2	6.992280975095422	AKORN, INC.
--AKRX	3	10.205947383541092	AKORN, INC.
--AKRX	4	41.1988181040794	AKORN, INC.
CREATE TABLE variazionePercentuale AS
SELECT a.ticker,a.mese,(((b.close-a.close)/a.close) * 100) AS variazione , a.name
FROM firstCLose AS a , lastCLose AS b
WHERE a.ticker=b.ticker AND a.mese=b.mese; 

--query finale 

SELECT 
    a1.name AS name1,
    a2.name AS name2, 
    a1.mese, 
    a1.variazione as variazione1,
	a2.variazione as variazione2
FROM variazionePercentuale AS a1 JOIN  variazionePercentuale AS a2  ON
       a1.mese= a2.mese 
WHERE a1.name!=a2.name AND (a1.variazione-a2.variazione<1) AND (a1.variazione-a2.variazione>0)
SORT BY name1,name2 DESC;






