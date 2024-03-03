from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.api_loader import ApiLoader
from InvestmentAsCode_AssetFlowPlatform.data_processing.savers.mongo_saver import MongoSaver

# Load the data from API
loader_config = {
  "api_url": "https://financialmodelingprep.com/api/v3/stock/list",
  "api_key_name": "FMP_API_KEY",
  "parameters": {
    "query": "AA"
  }
}

api_loader = ApiLoader(loader_config)
data = api_loader.fetch_data()

# Save the data into MongoDB
saver_config= {
    "database_name": "ingestion_database",
    "collection_name": "stock_list"
}

saver = MongoSaver(saver_config)
saver.save_data(data)

