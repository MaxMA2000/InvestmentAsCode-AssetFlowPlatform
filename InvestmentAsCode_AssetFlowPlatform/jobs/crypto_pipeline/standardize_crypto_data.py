import os
import sys
from datetime import date, datetime
import psycopg2
from typing import List, Dict, Any, Literal

from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.mongo_loader import (
    MongoLoader,
)
from InvestmentAsCode_AssetFlowPlatform.data_processing.managers.postgres_manager import (
    PostgresManager,
)

from dotenv import load_dotenv

load_dotenv()


def airflow_task(**kwargs):
    crypto_symbol = kwargs.get("crypto_symbol")
    task(crypto_symbol)


def task(crypto_symbol: str) -> None:
    """Main logic to get crypto info and crypto prices data from MongoDB and standardize it into Postgres Database

    Args:
        crypto_symbol (str): unique symbol defining crypto
    """

    crypto_info = get_crypto_info(crypto_symbol)

    # Establish a connection to the PostgreSQL database
    postgre_manager = PostgresManager()
    cursor = postgre_manager.cursor

    crypto_postgres_asset_id = standardize_crypto_info_to_postgres(
        cursor, crypto_symbol, crypto_info
    )

    to_be_added_crypto_price_data = get_to_be_added_crypto_price_data(
        cursor, crypto_symbol
    )

    standardize_crypto_price_to_postgresql(
        cursor, crypto_info, to_be_added_crypto_price_data, crypto_postgres_asset_id
    )

    # Commit the changes to the database
    postgre_manager.commit()
    postgre_manager.close()


def get_crypto_info(crypto_symbol: str) -> Dict[str, Any] | ValueError:
    """get the crypto info data from MongoDB

    Args:
        crypto_symbol (str): unique symbol defining crypto

    Raises:
        ValueError: there is no crypto_symbol in mongodb
        ValueError: there are multiple same crypto_symbols found in mongodb
        ValueError: unexpected cases

    Returns:
        Dict[str, Any] | ValueError: dictionary data with crypto info
    """

    mongo_crypto_list_config = {
        "database_name": "ingestion-general_info",
        "collection_name": "crypto_list",
    }
    mongo_crypto_list_loader = MongoLoader(mongo_crypto_list_config)

    crypto_in_mongo_crypto_list: List[Dict[str, Any]] = (
        mongo_crypto_list_loader.find_items_by_key_value("symbol", crypto_symbol)
    )

    if len(crypto_in_mongo_crypto_list) == 0:
        raise ValueError(
            f"'{crypto_symbol}' doesn't exist in the MongoDB 'ingestion-general_info'/ 'crypto_list'. Please Check "
        )

    if len(crypto_in_mongo_crypto_list) > 1:
        raise ValueError(
            f"'{crypto_symbol}' has multiple value in 'ingestion-general_info'/ 'crypto_list', instead it should have one  value only. Please Check "
        )

    if len(crypto_in_mongo_crypto_list) == 1:
        return crypto_in_mongo_crypto_list[0]
    else:
        raise ValueError(
            f"crypto_in_mongo_crypto_list has {len(crypto_in_mongo_crypto_list)} value, out of expectation handling. Please Check "
        )


def standardize_crypto_info_to_postgres(
    cursor: psycopg2.extensions.cursor, crypto_symbol: str, crypto_info: Dict[str, Any]
) -> tuple[int]:
    """standardize the crypto info data, write into postgres database

    Args:
        cursor (psycopg2.extensions.cursor): postgres database cursor
        crypto_symbol (str): unique symbol defining crypto
        crypto_info (Dict[str, Any]): crypto info data

    Raises:
        ValueError: there is multiple crypto asset_id in postgresql asset table

    Returns:
        tuple[int]: asset_id for this crypto in postgresql asset table
    """

    crypto_symbol = crypto_symbol
    crypto_name = crypto_info["name"]
    crypto_exchange = crypto_info["stockExchange"]
    crypto_exchangeShortName = crypto_info["exchangeShortName"]
    crypto_type = "crypto"
    crypto_date = crypto_info["date"]

    # Check if a row with the same symbol exists
    cursor.execute("SELECT asset_id FROM asset WHERE symbol = %s", (crypto_symbol,))
    existing_asset_id = cursor.fetchone()
    print(f"existing_asset_id = {existing_asset_id}")

    if existing_asset_id is None:
        # Insert the new row
        cursor.execute(
            "INSERT INTO asset (symbol, name, exchange, exchange_short_name, type, as_of_date) VALUES (%s, %s, %s, %s, %s, %s) RETURNING asset_id",
            (
                crypto_symbol,
                crypto_name,
                crypto_exchange,
                crypto_exchangeShortName,
                crypto_type,
                crypto_date,
            ),
        )
        new_asset_id = cursor.fetchone()[0]
        print("New asset inserted with asset_id:", new_asset_id)
        crypto_postgres_asset_id = new_asset_id
    elif existing_asset_id is not None:
        # Update the existing row
        cursor.execute(
            "UPDATE asset SET name = %s, exchange = %s, exchange_short_name = %s, type = %s, as_of_date = %s WHERE asset_id = %s",
            (
                crypto_name,
                crypto_exchange,
                crypto_exchangeShortName,
                crypto_type,
                crypto_date,
                existing_asset_id[0],
            ),
        )
        print("Existing asset updated.")
        crypto_postgres_asset_id = existing_asset_id
    else:
        raise ValueError(
            f"existing_asset_id should only has 0/1 value, instead it's having {existing_asset_id} value, meaning multiple '{crypto_symbol}' exists in PostgreSQL ASSET table. Please Check "
        )

    return crypto_postgres_asset_id


def get_to_be_added_crypto_price_data(
    cursor: psycopg2.extensions.cursor, crypto_symbol: str
) -> None:
    """get the crypto price data from mongodb and filter based on postgres crypto price data date range

    Args:
        cursor (psycopg2.extensions.cursor): postgres database cursor
        crypto_symbol (str): unique symbol defining crypto

    """

    # Execute the query to find min and max dates
    query = f"SELECT MIN(date), MAX(date) FROM crypto WHERE symbol = '{crypto_symbol}'"
    cursor.execute(query)

    # Fetch the result
    result = cursor.fetchone()
    postgres_crypto_min_date, postgres_crypto_max_date = result[0], result[1]

    mongo_crypto_price_config = {
        "database_name": "ingestion-crypto_price",
        "collection_name": crypto_symbol.upper(),
    }
    mongo_crypto_price_loader = MongoLoader(mongo_crypto_price_config)

    # Check if the result is None
    if postgres_crypto_min_date is None and postgres_crypto_max_date is None:
        print("No rows found for the given crypto symbol.")
        print("Inserting all crypto data from MongoDB into PostgreSQL...")
        to_be_added_crypto_price_data = (
            mongo_crypto_price_loader.load_data_with_checking()
        )
    else:
        postgres_crypto_min_date = datetime.strftime(postgres_crypto_min_date, "%Y-%m-%d")
        postgres_crypto_max_date = datetime.strftime(postgres_crypto_max_date, "%Y-%m-%d")

        print(f"PostgreSQL crypto '{crypto_symbol}' min_date = {postgres_crypto_min_date}")
        print(f"PostgreSQL crypto '{crypto_symbol}' max_date = {postgres_crypto_max_date}")

        # filter the mongodb data with the time period > postgresql max_date
        to_be_added_crypto_price_data: List[Dict] = (
            mongo_crypto_price_loader.filter_collection_by_date(
                "date", postgres_crypto_max_date, ">"
            )
        )

    return to_be_added_crypto_price_data


def standardize_crypto_price_to_postgresql(
    cursor: psycopg2.extensions.cursor,
    crypto_info: Dict[str, Any],
    to_be_added_crypto_price_data: List[Dict[str, Any]],
    crypto_postgres_asset_id: tuple,
) -> None:
    """take the crypto info data, to be added crypto price data, asset_id for crypto in asset table and insert into postgres database

    Args:
        cursor (psycopg2.extensions.cursor): postgres database cursor
        crypto_info (Dict[str, Any]): crypto info data
        to_be_added_crypto_price_data (List[Dict[str, Any]]): crypto price data to be added
        crypto_postgres_asset_id (tuple): asset_id for crypto in asset table

    """

    if len(to_be_added_crypto_price_data) == 0:
        print("No new crypto prices need to be added today, Finish the Program.")
    else:
        append_dates = [crypto["date"] for crypto in to_be_added_crypto_price_data]
        print(
            f"New crypto prices dates to be added from MongoDB to PostgreSQL: {str(append_dates)}"
        )

    for item in to_be_added_crypto_price_data:
        # Extract the necessary values from the dictionary
        asset_id = (
            crypto_postgres_asset_id  # Get the id from INSERT / UPDATE Asset Table
        )
        symbol = item["crypto_symbol"]
        crypto_date = item["date"]
        open_value = item["open"]
        high = item["high"]
        low = item["low"]
        close = item["close"]
        adj_close = item["adjClose"]
        volume = item["volume"]
        unadjusted_volume = item["unadjustedVolume"]
        change = item["change"]
        change_percent = item["changePercent"]
        vwap = item["vwap"]
        label = item["label"]
        change_over_time = item["changeOverTime"]
        as_of_date = crypto_info["date"]  # Get the date from INSERT / UPDATE Asset Table

        # Construct the SQL query
        query = """
      INSERT INTO crypto (
          asset_id, symbol, date, open, high, low, close, adj_close, volume,
          unadjusted_volume, change, change_percent, vwap, label, change_over_time, as_of_date
      ) VALUES (
          %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
      );
      """

        # Execute the SQL query
        cursor.execute(
            query,
            (
                asset_id,
                symbol,
                crypto_date,
                open_value,
                high,
                low,
                close,
                adj_close,
                volume,
                unadjusted_volume,
                change,
                change_percent,
                vwap,
                label,
                change_over_time,
                as_of_date,
            ),
        )


if __name__ == "__main__":
    task("BTCUSD")  # take BTCUSD as example
