
DROP TABLE IF EXISTS stock;
DROP TABLE IF EXISTS asset;
DROP TABLE IF EXISTS crypto;


-- Create Asset table
CREATE TABLE asset (
  asset_id SERIAL PRIMARY KEY,
  symbol VARCHAR(255),
  name VARCHAR(255),
  exchange VARCHAR(255),
  exchange_short_name VARCHAR(255),
  type VARCHAR(255),
  as_of_date DATE
);

-- Create Stock table
CREATE TABLE stock (
  asset_id INT,
  symbol VARCHAR(255),
  date DATE,
  open DECIMAL(10, 2),
  high DECIMAL(10, 2),
  low DECIMAL(10, 2),
  close DECIMAL(10, 2),
  adj_close DECIMAL(10, 2),
  volume INT,
  unadjusted_volume INT,
  change DECIMAL(10, 2),
  change_percent DECIMAL(10, 2),
  vwap FLOAT,
  label VARCHAR(255),
  change_over_time FLOAT,
  as_of_date DATE,
  PRIMARY KEY (asset_id, date),
  FOREIGN KEY (asset_id) REFERENCES Asset (asset_id)
);

-- Create Crypto table
CREATE TABLE crypto (
  asset_id INT,
  symbol VARCHAR(255),
  date DATE,
  open DECIMAL(18, 8),
  high DECIMAL(18, 8),
  low DECIMAL(18, 8),
  close DECIMAL(18, 8),
  adj_close DECIMAL(18, 8),
  volume BIGINT,
  unadjusted_volume BIGINT,
  change DECIMAL(18, 8),
  change_percent DECIMAL(10, 2),
  vwap DECIMAL(18, 8),
  label VARCHAR(255),
  change_over_time DECIMAL(18, 8),
  as_of_date DATE,
  PRIMARY KEY (asset_id, date),
  FOREIGN KEY (asset_id) REFERENCES Asset (asset_id)
);

-- Insert dummy data into asset table
INSERT INTO asset (symbol, name, exchange, exchange_short_name, type, as_of_date)
VALUES ('AAPL', 'Apple Inc.', 'NASDAQ', 'NASDAQ', 'Stock', '2024-03-10'),
      ('GOOGL', 'Alphabet Inc.', 'NASDAQ', 'NASDAQ', 'Stock', '2024-03-10'),
      ('BTC', 'Bitcoin', 'Crypto', 'Crypto', 'Cryptocurrency', '2024-03-10'),
      ('SPY', 'SPDR S&P 500 ETF Trust', 'NYSE', 'NYSE', 'ETF', '2024-03-10');

-- Insert dummy data into stock table
INSERT INTO stock (asset_id, symbol, date, open, high, low, close, adj_close, volume, unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date)
VALUES ((SELECT asset_id FROM Asset WHERE symbol = 'AAPL'), 'AAPL', '2024-03-10', 150.0, 155.0, 149.5, 152.0, 152.0, 100000, 100000, 2.0, 1.32, 151.0, 'AAPL', 0.02, '2024-03-10'),
      ((SELECT asset_id FROM Asset WHERE symbol = 'GOOGL'), 'GOOGL', '2024-03-10', 2500.0, 2550.0, 2495.0, 2530.0, 2530.0, 50000, 50000, 30.0, 1.19, 2510.0, 'GOOGL', 0.03, '2024-03-10'),
      ((SELECT asset_id FROM Asset WHERE symbol = 'SPY'), 'SPY', '2024-03-10', 400.0, 405.0, 398.0, 402.0, 402.0, 200000, 200000, 2.0, 0.5, 401.0, 'SPY', 0.005, '2024-03-10');

-- Insert dummy data into crypto table
INSERT INTO crypto (asset_id, symbol, date, open, high, low, close, adj_close, volume, unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date)
VALUES
(1, 'BTCUSD', '2024-03-20', 67610, 68124.11, 62410.78, 64151.15, 64151.15, 5836086720, 5836086720, -3458.85, -5.12, 65574.01, 'March 20, 24', -0.0512, '2024-03-20'),
(2, 'ETHUSD', '2024-03-20', 1832.45, 1860.12, 1805.78, 1820.36, 1820.36, 2956086720, 2956086720, -12.27, -0.67, 1835.12, 'March 20, 24', -0.0067, '2024-03-20');

SELECT * FROM asset;
SELECT * FROM stock;
SELECT * FROM crypto;
