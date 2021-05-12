DROP TABLE historical_stock_prices;
DROP TABLE firstAndLastData;
DROP TABLE firstPriceClose;
DROP TABLE lastPriceClose;
DROP TABLE variazione;
DROP TABLE output;

CREATE TABLE historical_stock_prices (ticker STRING, open float, close float, adj_close float,lowThe float, highThe float, volume float, dates date)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ',';

LOAD DATA LOCAL INPATH '/Users/seb/Desktop/BigData/daily-historical-stock-prices-1970-2018/historical_stock_prices.csv'
				OVERWRITE INTO TABLE historical_stock_prices;

--SELECT * FROM historical_stock_prices LIMIT 10;

CREATE TABLE firstAndLastData AS
SELECT ticker, min(dates) AS min_data , max(dates) AS max_data , min(lowThe) AS min_price , max(highThe) AS max_price
FROM historical_stock_prices
WHERE year(dates)>=2008
GROUP BY ticker;

SELECT * FROM firstAndLastData LIMIT 10;

CREATE TABLE firstPriceClose AS
SELECT data.ticker, data.min_data, hsp.close AS close
FROM firstAndLastData AS data, historical_stock_prices AS hsp
WHERE data.min_data=hsp.dates;

SELECT * FROM firstPriceClose LIMIT 10;

CREATE TABLE lastPriceClose AS
SELECT data.ticker, data.max_data, hsp.close AS close
FROM firstAndLastData AS data, historical_stock_prices AS hsp
WHERE data.max_data=hsp.dates;

SELECT * FROM lastPriceClose LIMIT 10;

CREATE TABLE variazione AS
SELECT vi.ticker AS ticker, 
       (((vf.close-vi.close)/vi.close) * 100) AS var
FROM   firstPriceClose AS vi, lastPriceClose AS vf
WHERE  vi.ticker=vf.ticker;

SELECT * FROM variazione LIMIT 100;

-- Query per ottenere il risultato voluto
CREATE TABLE output AS
SELECT data.ticker,
       data.min_data, 
       data.max_data,
       variazione.var,
	data.min_price,
	data.max_price
FROM firstAndLastData AS data, variazione
WHERE data.ticker=variazione.ticker
SORT BY data.max_data DESC;

SELECT * FROM output LIMIT 100;


--DROP TABLE historical_stock_prices;
--DROP TABLE firstAndLastData;
--DROP TABLE firstPriceClose;
--DROP TABLE lastPriceClose;
--DROP TABLE variazione;
--DROP TABLE output;