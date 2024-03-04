# Import Loaders & Savers
from InvestmentAsCode_AssetFlowPlatform.data_processing.loaders.api_loader import ApiLoader
from InvestmentAsCode_AssetFlowPlatform.data_processing.savers.mongo_saver import MongoSaver

from InvestmentAsCode_AssetFlowPlatform.utils.common_utils import add_date_to_data

###################################
# Load Data
###################################

# Load the data from API
api_loader_config = {
  "api_url": "https://financialmodelingprep.com/api/v3/search",
  "api_key_name": "FMP_API_KEY",
  "parameters": {
    "query": "AA"
  }
}

api_loader = ApiLoader(api_loader_config)
new_data = api_loader.fetch_data()

###################################
# Transform Data
###################################

new_data_with_date = add_date_to_data(new_data)

###################################
# Save Data
###################################

# Save the data into MongoDB
saver_config= {
    "database_name": "ingestion_database",
    "collection_name": "company_general_info"
}

saver = MongoSaver(saver_config)
saver.replace_collection(new_data_with_date)
