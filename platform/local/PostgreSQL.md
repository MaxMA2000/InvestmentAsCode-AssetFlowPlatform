

postgresql
- https://www.postgresql.org/download/macosx/

- to remove lock
  - `sudo rm /tmp/.s.PGSQL.5432.lock`
  - `sudo rm /tmp/.s.PGSQL.5432`

- start Postgres can click App icon
- Or using CLI:
  - `export PGDATA="/Users/maxma/Library/Application Support/Postgres/InvestmentAsCode/"`
  - `postgres`


- check port usage: `sudo lsof -i :8080`
- kill port application: `sudo kill -9 99213`

- To Set Up the PostgreSQL Database, do the following steps:
  1. create database "AssetFlowPlatform"
  2. Open Query Tools, run the following SQL Command:
  ```sql
  DROP TABLE IF EXISTS stock;
  DROP TABLE IF EXISTS asset;


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

  -- -- Insert dummy data into asset table
  -- INSERT INTO asset (symbol, name, exchange, exchange_short_name, type, as_of_date)
  -- VALUES ('AAPL', 'Apple Inc.', 'NASDAQ', 'NASDAQ', 'Stock', '2024-03-10'),
  --       ('GOOGL', 'Alphabet Inc.', 'NASDAQ', 'NASDAQ', 'Stock', '2024-03-10'),
  --       ('BTC', 'Bitcoin', 'Crypto', 'Crypto', 'Cryptocurrency', '2024-03-10'),
  --       ('SPY', 'SPDR S&P 500 ETF Trust', 'NYSE', 'NYSE', 'ETF', '2024-03-10');

  -- -- Insert dummy data into stock table
  -- INSERT INTO stock (asset_id, symbol, date, open, high, low, close, adj_close, volume, unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date)
  -- VALUES ((SELECT asset_id FROM Asset WHERE symbol = 'AAPL'), 'AAPL', '2024-03-10', 150.0, 155.0, 149.5, 152.0, 152.0, 100000, 100000, 2.0, 1.32, 151.0, 'AAPL', 0.02, '2024-03-10'),
  --       ((SELECT asset_id FROM Asset WHERE symbol = 'GOOGL'), 'GOOGL', '2024-03-10', 2500.0, 2550.0, 2495.0, 2530.0, 2530.0, 50000, 50000, 30.0, 1.19, 2510.0, 'GOOGL', 0.03, '2024-03-10'),
  --       ((SELECT asset_id FROM Asset WHERE symbol = 'SPY'), 'SPY', '2024-03-10', 400.0, 405.0, 398.0, 402.0, 402.0, 200000, 200000, 2.0, 0.5, 401.0, 'SPY', 0.005, '2024-03-10');

  SELECT * FROM asset;

  SELECT * FROM stock;
  ```
