from typing import List, Dict, Any
import sys
import os
from datetime import date, datetime
import psycopg2
from dotenv import load_dotenv

from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.mongo_loader import MongoLoader

load_dotenv()


# step 1: load stock_prices to get list of stocks (from mongo)
# get list of stock collections in MongoDB
mongo_stock_price_loader = MongoLoader.create_without_parameters()
mongo_stock_price_loader.database_name = "ingestion-stock_price"
ingested_stock_lists = mongo_stock_price_loader.get_collection_list_from_database()

print(ingested_stock_lists)

# step 2: store the list of stocks, for each stock inside, run the logic following:
# stock = ingested_stock_lists[0]
for stock in ingested_stock_lists:
  print(stock)

  # step 3: based on this stock symbol, get stock info  (from mongo)
  mongo_stock_price_loader.collection_name = stock
  stock_symbol = mongo_stock_price_loader.get_unique_value("stock_symbol")
  print(f"symbol = {stock_symbol}")

  if len(stock_symbol) > 1:
    raise ValueError(f"There should only 1 unique symbol for Stock: '{stock}', while instead stock_symbol = {stock_symbol} ")

  stock_symbol = stock_symbol[0]

  # step 4: insert / update stock info into postgre asset table
  mongo_stock_info_loader = MongoLoader({"database_name": "ingestion-general_info", "collection_name": "stock_list"})
  stock_info = mongo_stock_info_loader.find_items_by_key_value("symbol", stock_symbol)
  print(stock_info)
  mongo_stock_info_loader.manager.release_connection(mongo_stock_info_loader.client)

  if len(stock_info) > 1:
    raise ValueError(f"There should only 1 unique item for Stock, while instead there are {len(stock_info)} items, Please Check")

  stock_info = stock_info[0]


  # Establish a connection to the PostgreSQL database
  conn = psycopg2.connect(
      host = os.getenv("POSTGRES_HOST"),
      port = os.getenv("POSTGRES_PORT"),
      database = os.getenv("POSTGRES_DATABASE"),
      user = os.getenv("POSTGRES_USER"),
      password = os.getenv("POSTGRES_PASSWORD")
  )

  # Create a cursor object to execute SQL queries
  cur = conn.cursor()

  # Check if a row with the same symbol exists
  cur.execute("SELECT asset_id FROM asset WHERE symbol = %s", (stock_symbol,))
  existing_asset_id = cur.fetchone()
  print(f"existing_asset_id = {existing_asset_id}")


  # step 5: get the inserted asset id, combined with the stock prices
  if existing_asset_id:
      # Update the existing row
      cur.execute("UPDATE asset SET name = %s, exchange = %s, exchange_short_name = %s, type = %s, as_of_date = %s WHERE asset_id = %s",
                  (stock_info['name'], stock_info['exchange'], stock_info['exchangeShortName'], stock_info['type'], stock_info['date'], existing_asset_id[0]))
      print("Existing asset updated.")
      stock_postgres_asset_id = existing_asset_id
  else:
      # Insert the new row
      cur.execute("INSERT INTO asset (symbol, name, exchange, exchange_short_name, type, as_of_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING asset_id",
              (stock_info['symbol'], stock_info['name'], stock_info['exchange'], stock_info['exchangeShortName'], stock_info['type'], stock_info['date']))
      new_asset_id = cur.fetchone()[0]
      print("New asset inserted with asset_id:", new_asset_id)
      stock_postgres_asset_id = new_asset_id


  # step 6: format the stock daily price data, append the stock table

  mongo_stock_min_date, mongo_stock_max_date = mongo_stock_price_loader.get_collection_min_max_dates('date')
  print(f"MongoDB stock '{stock_symbol}' min_date = {mongo_stock_min_date}")
  print(f"MongoDB stock '{stock_symbol}' max_date = {mongo_stock_max_date}")


  # Execute the query to find min and max dates
  query = f"SELECT MIN(as_of_date), MAX(as_of_date) FROM stock WHERE symbol = '{stock_symbol}'"
  cur.execute(query)

  # Fetch the result
  result = cur.fetchone()
  postgres_stock_min_date, postgres_stock_max_date = result[0], result[1]

  # Check if the result is None
  if postgres_stock_min_date is None and postgres_stock_max_date is None:
      print("No rows found for the given stock symbol.")
      print("Inserting all stock data from MongoDB into PostgreSQL...")
      to_be_added_stock_price_data = mongo_stock_price_loader.load_data()
  else:
    postgres_stock_min_date = datetime.strftime(postgres_stock_min_date, '%Y-%m-%d')
    postgres_stock_max_date = datetime.strftime(postgres_stock_max_date, '%Y-%m-%d')


    print('Minimum Date:', postgres_stock_min_date)
    print('Maximum Date:', postgres_stock_max_date)

    print(f"PostgreSQL stock '{stock_symbol}' min_date = {postgres_stock_min_date}")
    print(f"PostgreSQL stock '{stock_symbol}' max_date = {postgres_stock_max_date}")

    # filter the mongodb data with the time period > postgresql max_date
    to_be_added_stock_price_data: List[Dict] = mongo_stock_price_loader.filter_collection_by_date('date', postgres_stock_max_date, ">")

  if to_be_added_stock_price_data == []:
    print("No new stock prices need to be added today, Finish the Program.")
    sys.exit()

  append_dates = [stock["date"] for stock in to_be_added_stock_price_data]
  print(f"New stock prices dates to be added from MongoDB to PostgreSQL: {str(append_dates)}")


  for item in to_be_added_stock_price_data:
      # Extract the necessary values from the dictionary
      asset_id = stock_postgres_asset_id  # Get the id from INSERT / UPDATE Asset Table
      symbol = item['stock_symbol']
      stock_date = item['date']
      open_value = item['open']
      high = item['high']
      low = item['low']
      close = item['close']
      adj_close = item['adjClose']
      volume = item['volume']
      unadjusted_volume = item['unadjustedVolume']
      change = item['change']
      change_percent = item['changePercent']
      vwap = item['vwap']
      label = item['label']
      change_over_time = item['changeOverTime']
      as_of_date = stock_info['date']  # Get the date from INSERT / UPDATE Asset Table

      # Construct the SQL query
      query = """
      INSERT INTO stock (
          asset_id, symbol, date, open, high, low, close, adj_close, volume,
          unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date
      ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
      """

      # Execute the SQL query
      cur.execute(query, (
          asset_id, symbol, stock_date, open_value, high, low, close, adj_close, volume,
          unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date
      ))


  # Commit the changes to the database
  conn.commit()
  cur.close()
  conn.close()
