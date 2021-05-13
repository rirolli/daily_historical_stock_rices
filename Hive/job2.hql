DROP TABLE historical_stock_prices;
DROP TABLE sectors;
DROP TABLE sectorYears;
DROP TABLE firstAndLastData;
DROP TABLE minClose;
DROP TABLE maxClose;
DROP TABLE firstAzioneClose;
DROP TABLE lastAzioneClose;
DROP TABLE variazioneSettore;
DROP TABLE variazioneAzione;
DROP TABLE volume;


CREATE TABLE historical_stock_prices (ticker STRING, open float, close float, adj_close float,lowThe float, highThe float, volume float, dates date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
				OVERWRITE INTO TABLE historical_stock_prices;

SELECT * FROM historical_stock_prices LIMIT 100;

CREATE TABLE sectors (ticker STRING, exchage STRING, name STRING, sector STRING,industry STRING)
ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.OpenCSVSerde';

LOAD DATA LOCAL INPATH '/Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stocks.csv'
				OVERWRITE INTO TABLE sectors;

SELECT * FROM sectors LIMIT 100;

CREATE TABLE sectorYears AS
SELECT hsp.ticker AS ticker, sectors.sector AS sector, hsp.dates AS data, hsp.close AS close,hsp.volume AS volume
FROM sectors join historical_stock_prices AS hsp ON sectors.ticker=hsp.ticker
WHERE YEAR(hsp.dates) >= '2016';

SELECT * FROM sectorYears LIMIT 1000;

CREATE TABLE firstAndLastData AS
SELECT 
    sector,
    ticker, 
    min(data) AS min_data, 
    max(data) AS max_data 
FROM sectorYears 
GROUP BY sector, ticker, YEAR(data);

SELECT * FROM firstAndLastData LIMIT 1000;

CREATE TABLE minClose AS 
SELECT 
    d.sector AS sector, 
    YEAR(d.min_data) AS anno, 
    SUM(a.close) AS min_close 
FROM sectorYears AS a, firstAndLastData AS d
WHERE a.sector=d.sector AND a.data=d.min_data AND d.ticker=a.ticker 
GROUP BY d.sector, YEAR(d.min_data);

SELECT * FROM minClose LIMIT 1000;

CREATE TABLE maxClose AS 
SELECT 
    d.sector AS sector, 
    YEAR(d.max_data) AS anno, 
    SUM(a.close) AS max_close 
FROM sectorYears AS a, firstAndLastData AS d
WHERE a.sector=d.sector AND a.data=d.max_data AND d.ticker=a.ticker 
GROUP BY d.sector, YEAR(d.max_data);

SELECT * FROM maxClose LIMIT 1000;

-- primo prezzo di chiusura per ogni azione 
CREATE TABLE firstAzioneClose AS
SELECT a.sector AS sector, a.ticker AS ticker, YEAR(a.min_data) AS data, b.close AS close
FROM   firstAndLastData AS a, sectorYears AS b
WHERE  a.sector=b.sector AND a.min_data=b.data AND a.ticker=b.ticker;

SELECT * FROM firstAzioneClose LIMIT 1000;

-- ultimo prezzo di chiusura per ogni azione 
CREATE TABLE lastAzioneClose AS
SELECT a.sector AS sector, a.ticker AS ticker, YEAR(a.max_data) AS data, b.close AS close
FROM   firstAndLastData AS a , sectorYears as b
WHERE  a.sector=b.sector AND a.max_data=b.data AND a.ticker=b.ticker;

SELECT * FROM lastAzioneClose LIMIT 1000;

CREATE TABLE variazioneSettore AS
SELECT a.sector, (((b.max_close-a.min_close)/a.min_close) * 100) AS varSettore , a.anno AS anno
FROM minClose AS a, maxClose AS b
WHERE a.anno=b.anno AND a.sector=b.sector
ORDER BY a.sector, anno;

SELECT * FROM variazioneSettore LIMIT 1000;

CREATE TABLE variazioneAzione AS
SELECT a.sector, a.ticker, (((b.close-a.close)/a.close) * 100) AS varAzione, a.data AS anno
FROM firstAzioneClose AS a, lastAzioneClose AS b
WHERE a.data=b.data AND a.ticker=b.ticker;

SELECT * FROM variazioneAzione LIMIT 1000;

-- per ogni azione di un settore sommo il volume di transazioni nell'anno 
CREATE TABLE volume AS
SELECT ticker, sector, YEAR(data) AS anno, SUM(volume) AS volume
FROM sectorYears
GROUP BY ticker,sector,YEAR(data);

SELECT * FROM volume LIMIT 1000;

--query finale 
SELECT a.sector, a.varSettore,b.ticker, max(varAzione) ,c.ticker, max(c.volume), a.anno
FROM variazioneSettore AS a, variazioneAzione AS b, volume AS c
WHERE a.sector=b.sector AND b.sector=c.sector AND a.anno=b.anno AND b.anno=c.anno;





