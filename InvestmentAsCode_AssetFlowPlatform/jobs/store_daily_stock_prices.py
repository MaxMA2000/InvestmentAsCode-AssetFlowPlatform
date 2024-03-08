from typing import List, Dict, Any
import sys

# Import Loaders & Savers
from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.api_loader import (
    ApiLoader,
)
from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.mongo_loader import (
    MongoLoader,
)
from InvestmentAsCode_AssetFlowPlatform.data_processing.managers.mongo_manager import (
    MongoDBManager,
)
from InvestmentAsCode_AssetFlowPlatform.data_processing.savers.mongo_saver import (
    MongoSaver,
)

from InvestmentAsCode_AssetFlowPlatform.utils.common_utils import (
    find_min_max_dates,
)



def airflow_task(**kwargs):
  stock_symbol = kwargs.get("stock_symbol")
  task(stock_symbol)


def task(stock_symbol):

    check_stock_symbol_exist(stock_symbol)

    stock_historical_price_data: List[Dict[str, Any]] = fetch_stock_daily_price(
        stock_symbol
    )
    is_stock_exist_in_collection = check_if_stock_collection_exists(stock_symbol)

    if is_stock_exist_in_collection:
        transformed_stock_price_data = transform_data_if_stock_collection_exist(
            stock_symbol, stock_historical_price_data
        )
    else:
        transformed_stock_price_data = keep_entire_historical_price_data(
            stock_symbol, stock_historical_price_data
        )
    new_stock_prices_to_add = add_symbol_to_stock_price_data(stock_symbol, transformed_stock_price_data)
    save_data_to_mongo_db(stock_symbol, new_stock_prices_to_add)


def check_stock_symbol_exist(stock_symbol: str) -> ValueError | None:
    # Load the data from MongoDB
    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-general_info", "collection_name": "stock_list"}
    )

    existing_stock_list: List[str] = [
        stock["symbol"] for stock in mongo_general_info_loader.load_data_with_checking()
    ]

    if stock_symbol not in existing_stock_list:
        raise ValueError(
            f"Stock Symbol '{stock_symbol}' is not founded in the ingestion-general_info/stock_list, End Program... Please Check."
        )

    print(
        f" Found Stock Symbol '{stock_symbol} in ingestion-general_info/stock_list, start checking if it is existed in ingestion-stock_price/{stock_symbol}"
    )


def fetch_stock_daily_price(stock_symbol: str) -> List[Dict[str, Any]] | str:
    # Load the data from API
    api_loader_config = {
        "api_url": f"https://financialmodelingprep.com/api/v3/historical-price-full/{stock_symbol}",
        "api_key_name": "FMP_API_KEY",
        "parameters": {},
    }

    api_loader = ApiLoader(api_loader_config)
    new_data = api_loader.fetch_data()

    stock_daily_prices_data: List[Dict[str, Any]] = new_data["historical"]

    return stock_daily_prices_data


def check_if_stock_collection_exists(stock_symbol: str) -> bool:

    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-stock_price", "collection_name": stock_symbol}
    )

    is_target_stock_collection_exist = MongoDBManager.check_collection_exists(
        mongo_general_info_loader.client, "ingestion-stock_price", stock_symbol
    )

    return is_target_stock_collection_exist


def transform_data_if_stock_collection_exist(
    stock_symbol: str, stock_historical_price_data: List[Dict[str, Any]]
):
    print(
        f"'{stock_symbol}' collection exist in the ingestion-stock_list database, will fetch missing dates prices"
    )

    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-stock_price", "collection_name": stock_symbol}
    )

    min_date, max_date = mongo_general_info_loader.get_collection_min_max_dates("date")
    print(
        f"'{stock_symbol}' Current Date range in existing collection: {min_date} to {max_date}"
    )

    new_data_min_date, new_data_max_date = find_min_max_dates(
        stock_historical_price_data, "date"
    )
    print(
        f"'{stock_symbol}' Current Date range from new API Fetching: {new_data_min_date} to {new_data_max_date}"
    )

    # filter the fetched data with the time period > existing max_date
    new_stock_prices_to_add = [
        item
        for item in stock_historical_price_data
        if max_date < item["date"] and item["date"] != max_date
    ]

    append_dates = [stock["date"] for stock in new_stock_prices_to_add]
    print(f"New stock prices dates to be added: {str(append_dates)}")
    if len(append_dates) == 0:
        print("Stop the task as no new stock prices to be added")
        sys.exit()

    return new_stock_prices_to_add


def keep_entire_historical_price_data(stock_symbol: str, stock_historical_price_data):
    new_stock_prices_to_add = stock_historical_price_data
    print(
        f"'{stock_symbol}' collection doesn't exist in the ingestion-stock_list database, will fetch entire dates prices"
    )
    return new_stock_prices_to_add


def add_symbol_to_stock_price_data(stock_symbol: str, stock_historical_price_data):
      for item in stock_historical_price_data:
        item['stock_symbol'] = stock_symbol
      return stock_historical_price_data


def save_data_to_mongo_db(stock_symbol: str, new_stock_prices_to_add) -> None:
    # Save the data into MongoDB
    saver_config = {
        "database_name": "ingestion-stock_price",
        "collection_name": stock_symbol,
    }

    saver = MongoSaver(saver_config)
    saver.save_data(new_stock_prices_to_add)


if __name__ == "__main__":
    task()
