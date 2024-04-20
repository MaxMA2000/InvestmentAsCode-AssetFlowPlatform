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


def task(stock_symbol: str) -> None:
    """Main logic to fetch 'stock_symbol' stock prices, transform and write into MongoDB

    Args:
        stock_symbol (str): unique symbol defining stock
    """

    check_stock_symbol_exist(stock_symbol)

    stock_historical_price_data: List[Dict[str, Any]] = fetch_stock_daily_price(stock_symbol)

    is_stock_exist_in_collection = check_if_stock_collection_exists(stock_symbol)

    stock_price_data = get_stock_price_data(is_stock_exist_in_collection, stock_symbol, stock_historical_price_data)

    if stock_price_data is not None:
      new_stock_prices_to_add = add_symbol_to_stock_price_data(stock_symbol, stock_price_data)
      save_data_to_mongo_db(stock_symbol, new_stock_prices_to_add)
    else:
      print(f"Skip saving data as there is no new stock to add for {stock_symbol}")


def check_stock_symbol_exist(stock_symbol: str) -> ValueError | None:
    """Check if the stock_symbol exists in the "ingestion-general_info/stock_list"

    Args:
        stock_symbol (str): unique symbol defining stock

    Raises:
        ValueError: raise the error when stock symbol is not founded in stock_list collection,
                    meaning that source API doesn't support this stock, or stock_symbol is wrong

    Returns:
        ValueError | None: pass the checking and avoid raising error if found stock_symbol in stock_list collection
    """
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
    """Send API to fetch daily stock prices for stock_symbol

    Args:
        stock_symbol (str): unique symbol defining stock

    Returns:
        List[Dict[str, Any]] | str: a list of dictionary, each containing stock prices collection for a given trading date
    """
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
    """To check if the collection already exists in current MongoDB "ingestion-stock_price" datbase

    Args:
        stock_symbol (str): unique symbol defining stock

    Returns:
        bool: True / False representing whether collection exists already
    """

    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-stock_price", "collection_name": stock_symbol}
    )

    is_target_stock_collection_exist = MongoDBManager.check_collection_exists(
        mongo_general_info_loader.client, "ingestion-stock_price", stock_symbol
    )

    return is_target_stock_collection_exist



def get_stock_price_data(is_stock_exist_in_collection, stock_symbol, stock_historical_price_data):
    if is_stock_exist_in_collection:
        print(f"'{stock_symbol}' collection exist in the ingestion-stock_list database, will fetch missing dates prices")

        mongo_general_info_loader = MongoLoader(
            {"database_name": "ingestion-stock_price", "collection_name": stock_symbol}
        )

        mongo_min_date, mongo_max_date = (
            mongo_general_info_loader.get_collection_min_max_dates("date")
        )
        print(f"'{stock_symbol}' Date range in existing MongoDB collection: {mongo_min_date} to {mongo_max_date}")

        # api_data_min_date, api_data_max_date = find_min_max_dates(stock_historical_price_data, "date")
        # print(f"'{stock_symbol}' Date range from new API Fetching: {api_data_min_date} to {api_data_max_date}")

        print(f" Filtering new api fetched data with only 'date' > {mongo_max_date} ")

        # filter the fetched data with the time period > existing max_date
        new_stock_prices_to_add = [
            item
            for item in stock_historical_price_data
            if mongo_max_date < item["date"] and item["date"] != mongo_max_date
        ]

        append_dates = [stock["date"] for stock in new_stock_prices_to_add]
        print(f"New stock prices dates to be added: {str(append_dates)}")
        if len(append_dates) == 0:
            print("There is no new stock prices to be added")
            return None

        stock_price_data = new_stock_prices_to_add
    else:
        print(f"'{stock_symbol}' collection doesn't exist in the ingestion-stock_list database, will ingest entire fetched dates prices")
        stock_price_data = stock_historical_price_data

    return stock_price_data


def add_symbol_to_stock_price_data(
    stock_symbol: str, stock_historical_price_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """_summary_

    Args:
        stock_symbol (str): unique symbol defining stock
        stock_historical_price_data (List[Dict[str, Any]]): updated list of dictionaries containing stock historical data

    Returns:
        List[Dict[str, Any]]: list of dictionaries, with each dictionary added {'stock_symbol': XX}
    """
    for item in stock_historical_price_data:
        item["stock_symbol"] = stock_symbol
    return stock_historical_price_data


def save_data_to_mongo_db(
    stock_symbol: str, new_stock_prices_to_add: List[Dict[str, Any]]
) -> None:
    """save the transformed stock price data into MongoDB

    Args:
        stock_symbol (str): unique symbol defining stock
        new_stock_prices_to_add (List[Dict[str, Any]]): updated list of dictionaries containing stock historical data
    """
    # Save the data into MongoDB
    saver_config = {
        "database_name": "ingestion-stock_price",
        "collection_name": stock_symbol.upper(),
    }

    saver = MongoSaver(saver_config)
    saver.save_data(new_stock_prices_to_add)


if __name__ == "__main__":
    task("AAPL")  # take AAPL as example
