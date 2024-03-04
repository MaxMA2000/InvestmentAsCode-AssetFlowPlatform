import os
from pymongo import MongoClient
from typing import Dict, Any, Callable

from .loader import Loader
from InvestmentAsCode_AssetFlowPlatform.data_processing.managers.mongo_manager import MongoDBManager

from dotenv import load_dotenv
load_dotenv()

class MongoLoader(Loader):

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)

        self.database_name = config.get('database_name')
        self.collection_name = config.get('collection_name')

        self.manager = MongoDBManager()
        self.client = self.manager.connect_mongo_db()


    def fetch_data(self):
      pass

    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def load_data(self):
      db = self.client[self.database_name]
      collection = db[self.collection_name]

      # Load data from MongoDB
      print("Start loading data from MongoDB")
      data = list(collection.find())
      print("Finish loading data from MongoDB")

      return data
