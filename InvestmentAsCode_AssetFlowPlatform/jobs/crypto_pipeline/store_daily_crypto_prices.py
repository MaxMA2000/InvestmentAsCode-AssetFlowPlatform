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
    crypto_symbol = kwargs.get("crypto_symbol")
    task(crypto_symbol)


def task(crypto_symbol: str) -> None:
    """Main logic to fetch 'crypto_symbol' crypto prices, transform and write into MongoDB

    Args:
        crypto_symbol (str): unique symbol defining crypto
    """

    check_crypto_symbol_exist(crypto_symbol)

    crypto_historical_price_data: List[Dict[str, Any]] = fetch_crypto_daily_price(crypto_symbol)

    is_crypto_exist_in_collection = check_if_crypto_collection_exists(crypto_symbol)

    crypto_price_data = get_crypto_price_data(is_crypto_exist_in_collection, crypto_symbol, crypto_historical_price_data)

    if crypto_price_data is not None:
      new_crypto_prices_to_add = add_symbol_to_crypto_price_data(crypto_symbol, crypto_price_data)
      save_data_to_mongo_db(crypto_symbol, new_crypto_prices_to_add)
    else:
      print(f"Skip saving data as there is no new crypto to add for {crypto_symbol}")


def check_crypto_symbol_exist(crypto_symbol: str) -> ValueError | None:
    """Check if the crypto_symbol exists in the "ingestion-general_info/crypto_list"

    Args:
        crypto_symbol (str): unique symbol defining crypto

    Raises:
        ValueError: raise the error when crypto symbol is not founded in crypto_list collection,
                    meaning that source API doesn't support this crypto, or crypto_symbol is wrong

    Returns:
        ValueError | None: pass the checking and avoid raising error if found crypto_symbol in crypto_list collection
    """
    # Load the data from MongoDB
    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-general_info", "collection_name": "crypto_list"}
    )

    existing_crypto_list: List[str] = [
        crypto["symbol"] for crypto in mongo_general_info_loader.load_data_with_checking()
    ]

    if crypto_symbol not in existing_crypto_list:
        raise ValueError(
            f"crypto Symbol '{crypto_symbol}' is not founded in the ingestion-general_info/crypto_list, End Program... Please Check."
        )

    print(
        f" Found crypto Symbol '{crypto_symbol} in ingestion-general_info/crypto_list, start checking if it is existed in ingestion-crypto_price/{crypto_symbol}"
    )


def fetch_crypto_daily_price(crypto_symbol: str) -> List[Dict[str, Any]] | str:
    """Send API to fetch daily crypto prices for crypto_symbol

    Args:
        crypto_symbol (str): unique symbol defining crypto

    Returns:
        List[Dict[str, Any]] | str: a list of dictionary, each containing crypto prices collection for a given trading date
    """
    # Load the data from API
    api_loader_config = {
        "api_url": f"https://financialmodelingprep.com/api/v3/historical-price-full/{crypto_symbol}",
        "api_key_name": "FMP_API_KEY",
        "parameters": {},
    }

    api_loader = ApiLoader(api_loader_config)
    new_data = api_loader.fetch_data()

    crypto_daily_prices_data: List[Dict[str, Any]] = new_data["historical"]

    return crypto_daily_prices_data


def check_if_crypto_collection_exists(crypto_symbol: str) -> bool:
    """To check if the collection already exists in current MongoDB "ingestion-crypto_price" datbase

    Args:
        crypto_symbol (str): unique symbol defining crypto

    Returns:
        bool: True / False representing whether collection exists already
    """

    mongo_general_info_loader = MongoLoader(
        {"database_name": "ingestion-crypto_price", "collection_name": crypto_symbol}
    )

    is_target_crypto_collection_exist = MongoDBManager.check_collection_exists(
        mongo_general_info_loader.client, "ingestion-crypto_price", crypto_symbol
    )

    return is_target_crypto_collection_exist



def get_crypto_price_data(is_crypto_exist_in_collection, crypto_symbol, crypto_historical_price_data):
    if is_crypto_exist_in_collection:
        print(f"'{crypto_symbol}' collection exist in the ingestion-crypto_list database, will fetch missing dates prices")

        mongo_general_info_loader = MongoLoader(
            {"database_name": "ingestion-crypto_price", "collection_name": crypto_symbol}
        )

        mongo_min_date, mongo_max_date = (
            mongo_general_info_loader.get_collection_min_max_dates("date")
        )
        print(f"'{crypto_symbol}' Date range in existing MongoDB collection: {mongo_min_date} to {mongo_max_date}")

        # api_data_min_date, api_data_max_date = find_min_max_dates(crypto_historical_price_data, "date")
        # print(f"'{crypto_symbol}' Date range from new API Fetching: {api_data_min_date} to {api_data_max_date}")

        print(f" Filtering new api fetched data with only 'date' > {mongo_max_date} ")

        # filter the fetched data with the time period > existing max_date
        new_crypto_prices_to_add = [
            item
            for item in crypto_historical_price_data
            if mongo_max_date < item["date"] and item["date"] != mongo_max_date
        ]

        append_dates = [crypto["date"] for crypto in new_crypto_prices_to_add]
        print(f"New crypto prices dates to be added: {str(append_dates)}")
        if len(append_dates) == 0:
            print("There is no new crypto prices to be added")
            return None

        crypto_price_data = new_crypto_prices_to_add
    else:
        print(f"'{crypto_symbol}' collection doesn't exist in the ingestion-crypto_list database, will ingest entire fetched dates prices")
        crypto_price_data = crypto_historical_price_data

    return crypto_price_data


def add_symbol_to_crypto_price_data(
    crypto_symbol: str, crypto_historical_price_data: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:
    """_summary_

    Args:
        crypto_symbol (str): unique symbol defining crypto
        crypto_historical_price_data (List[Dict[str, Any]]): updated list of dictionaries containing crypto historical data

    Returns:
        List[Dict[str, Any]]: list of dictionaries, with each dictionary added {'crypto_symbol': XX}
    """
    for item in crypto_historical_price_data:
        item["crypto_symbol"] = crypto_symbol
    return crypto_historical_price_data


def save_data_to_mongo_db(
    crypto_symbol: str, new_crypto_prices_to_add: List[Dict[str, Any]]
) -> None:
    """save the transformed crypto price data into MongoDB

    Args:
        crypto_symbol (str): unique symbol defining crypto
        new_crypto_prices_to_add (List[Dict[str, Any]]): updated list of dictionaries containing crypto historical data
    """
    # Save the data into MongoDB
    saver_config = {
        "database_name": "ingestion-crypto_price",
        "collection_name": crypto_symbol.upper(),
    }

    saver = MongoSaver(saver_config)
    saver.save_data(new_crypto_prices_to_add)


if __name__ == "__main__":
    task("BTCUSD")  # take BTCUSD as example
