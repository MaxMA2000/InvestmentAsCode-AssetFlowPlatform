import os
import sys
from datetime import date, datetime
import psycopg2
from typing import List, Dict, Any, Literal

from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.mongo_loader import MongoLoader
from InvestmentAsCode_AssetFlowPlatform.data_processing.managers.postgres_manager import PostgresManager

from dotenv import load_dotenv
load_dotenv()



def airflow_task(**kwargs):
    stock_symbol = kwargs.get("stock_symbol")
    task(stock_symbol)


def task(stock_symbol: str) -> None:
  """ Main logic to get stock info and stock prices data from MongoDB and standardize it into Postgres Database

  Args:
      stock_symbol (str): unique symbol defining stock
  """

  stock_info = get_stock_info(stock_symbol)

  # Establish a connection to the PostgreSQL database
  postgre_manager = PostgresManager()
  cursor = postgre_manager.cursor

  stock_postgres_asset_id = standardize_stock_info_to_postgres(cursor, stock_symbol, stock_info)

  to_be_added_stock_price_data = get_to_be_added_stock_price_data(cursor, stock_symbol)

  standardize_stock_price_to_postgresql(cursor, stock_info, to_be_added_stock_price_data, stock_postgres_asset_id)

  # Commit the changes to the database
  postgre_manager.commit()
  postgre_manager.close()




def get_stock_info(stock_symbol: str) -> Dict[str, Any] | ValueError:
  """ get the stock info data from MongoDB

  Args:
      stock_symbol (str): unique symbol defining stock

  Raises:
      ValueError: there is no stock_symbol in mongodb
      ValueError: there are multiple same stock_symbols found in mongodb
      ValueError: unexpected cases

  Returns:
      Dict[str, Any] | ValueError: dictionary data with stock info
  """

  mongo_stock_list_config = {"database_name": "ingestion-general_info", "collection_name": "stock_list"}
  mongo_stock_list_loader = MongoLoader(mongo_stock_list_config)

  stock_in_mongo_stock_list: List[Dict[str, Any]] = mongo_stock_list_loader.find_items_by_key_value("symbol", stock_symbol)

  if len(stock_in_mongo_stock_list) == 0:
    raise ValueError(f"'{stock_symbol}' doesn't exist in the MongoDB 'ingestion-general_info'/ 'stock_list'. Please Check ")

  if len(stock_in_mongo_stock_list) > 1:
    raise ValueError(f"'{stock_symbol}' has multiple value in 'ingestion-general_info'/ 'stock_list', instead it should have one  value only. Please Check ")

  if len(stock_in_mongo_stock_list) == 1:
    return stock_in_mongo_stock_list[0]
  else:
    raise ValueError(f"stock_in_mongo_stock_list has {len(stock_in_mongo_stock_list)} value, out of expectation handling. Please Check ")


def standardize_stock_info_to_postgres(cursor: psycopg2.extensions.cursor, stock_symbol: str, stock_info: Dict[str, Any]) -> tuple[int]:
  """standardize the stock info data, write into postgres database

  Args:
      cursor (psycopg2.extensions.cursor): postgres database cursor
      stock_symbol (str): unique symbol defining stock
      stock_info (Dict[str, Any]): stock info data

  Raises:
      ValueError: there is multiple stock asset_id in postgresql asset table

  Returns:
      tuple[int]: asset_id for this stock in postgresql asset table
  """

  stock_symbol = stock_symbol
  stock_name = stock_info['name']
  stock_exchange = stock_info['exchange']
  stock_exchangeShortName = stock_info['exchangeShortName']
  stock_type = stock_info['type']
  stock_date = stock_info['date']


  # Check if a row with the same symbol exists
  cursor.execute("SELECT asset_id FROM asset WHERE symbol = %s", (stock_symbol,))
  existing_asset_id = cursor.fetchone()
  print(f"existing_asset_id = {existing_asset_id}")

  if existing_asset_id is None:
      # Insert the new row
      cursor.execute("INSERT INTO asset (symbol, name, exchange, exchange_short_name, type, as_of_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING asset_id",
              (stock_symbol, stock_name, stock_exchange, stock_exchangeShortName, stock_type, stock_date))
      new_asset_id = cursor.fetchone()[0]
      print("New asset inserted with asset_id:", new_asset_id)
      stock_postgres_asset_id = new_asset_id
  elif existing_asset_id is not None:
      # Update the existing row
      cursor.execute("UPDATE asset SET name = %s, exchange = %s, exchange_short_name = %s, type = %s, as_of_date = %s WHERE asset_id = %s",
                  (stock_name, stock_exchange, stock_exchangeShortName, stock_type, stock_date, existing_asset_id[0]))
      print("Existing asset updated.")
      stock_postgres_asset_id = existing_asset_id
  else:
    raise ValueError(f"existing_asset_id should only has 0/1 value, instead it's having {existing_asset_id} value, meaning multiple '{stock_symbol}' exists in PostgreSQL ASSET table. Please Check ")

  return stock_postgres_asset_id



def get_to_be_added_stock_price_data(cursor: psycopg2.extensions.cursor, stock_symbol: str) -> None:
  """ get the stock price data from mongodb and filter based on postgres stock price data date range

  Args:
      cursor (psycopg2.extensions.cursor): postgres database cursor
      stock_symbol (str): unique symbol defining stock

  """

  # Execute the query to find min and max dates
  query = f"SELECT MIN(as_of_date), MAX(as_of_date) FROM stock WHERE symbol = '{stock_symbol}'"
  cursor.execute(query)

  # Fetch the result
  result = cursor.fetchone()
  postgres_stock_min_date, postgres_stock_max_date = result[0], result[1]

  mongo_stock_price_config = {"database_name": "ingestion-stock_price", "collection_name": stock_symbol.upper()}
  mongo_stock_price_loader = MongoLoader(mongo_stock_price_config)

  # Check if the result is None
  if postgres_stock_min_date is None and postgres_stock_max_date is None:
    print("No rows found for the given stock symbol.")
    print("Inserting all stock data from MongoDB into PostgreSQL...")
    to_be_added_stock_price_data = mongo_stock_price_loader.load_data_with_checking()
  else:
    postgres_stock_min_date = datetime.strftime(postgres_stock_min_date, '%Y-%m-%d')
    postgres_stock_max_date = datetime.strftime(postgres_stock_max_date, '%Y-%m-%d')

    print(f"PostgreSQL stock '{stock_symbol}' min_date = {postgres_stock_min_date}")
    print(f"PostgreSQL stock '{stock_symbol}' max_date = {postgres_stock_max_date}")

    # filter the mongodb data with the time period > postgresql max_date
    to_be_added_stock_price_data: List[Dict] = mongo_stock_price_loader.filter_collection_by_date('date', postgres_stock_max_date, ">")

  return to_be_added_stock_price_data


def standardize_stock_price_to_postgresql(cursor: psycopg2.extensions.cursor, stock_info: Dict[str, Any], to_be_added_stock_price_data: List[Dict[str, Any]], stock_postgres_asset_id: tuple) -> None:
  """ take the stock info data, to be added stock price data, asset_id for stock in asset table and insert into postgres database

  Args:
      cursor (psycopg2.extensions.cursor): postgres database cursor
      stock_info (Dict[str, Any]): stock info data
      to_be_added_stock_price_data (List[Dict[str, Any]]): stock price data to be added
      stock_postgres_asset_id (tuple): asset_id for stock in asset table

  """

  if len(to_be_added_stock_price_data) == 0:
    print("No new stock prices need to be added today, Finish the Program.")
  else:
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
      cursor.execute(query, (
          asset_id, symbol, stock_date, open_value, high, low, close, adj_close, volume,
          unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date
      ))

if __name__ == "__main__":
    task("AAPL")  # take AAPL as example
