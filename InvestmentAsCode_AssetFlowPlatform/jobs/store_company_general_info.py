import os
from pymongo import MongoClient

from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.api_loader import ApiLoader


config = {
  "api_url": "https://financialmodelingprep.com/api/v3/search",
  "api_key_name": "FMP_API_KEY",
  "parameters": {
    "query": "AA"
  }
}


api_loader = ApiLoader(config)
data = api_loader.fetch_data()

print(type(data))

if type(data) == list:

    mongo_server_url = os.getenv('MONGO_SERVER_URL')
    database_name = "ingestion_database"
    collection_name = "company_general_info"


    # Create Mongo Client and connect with MongoDB Server
    client = MongoClient(mongo_server_url)

    db = client[database_name]
    collection = db[collection_name]

    # Insert each item into the collection
    print(f"start writing data..")
    for item in data:
        collection.insert_one(item)

    print(f"finish writing data..")


