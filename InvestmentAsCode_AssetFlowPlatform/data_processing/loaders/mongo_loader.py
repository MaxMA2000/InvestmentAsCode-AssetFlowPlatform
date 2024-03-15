import os
import sys
from datetime import datetime
from typing import Dict, Any, Callable
from pymongo import MongoClient

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

    @classmethod
    def create_without_parameters(cls):
        config = {
            'database_name': 'admin',
            'collection_name': 'admin'
        }
        return cls(config)

    @property
    def database_name(self):
        return self._database_name

    @database_name.setter
    def database_name(self, value):
        self._database_name = value

    @property
    def collection_name(self):
        return self._collection_name

    @collection_name.setter
    def collection_name(self, value):
        self._collection_name = value


    def fetch_data(self):
      pass


    def load_data(self):
      db = self.client[self.database_name]
      collection = db[self.collection_name]

      # Load data from MongoDB
      print("Start loading data from MongoDB")
      data = list(collection.find())
      print("Finish loading data from MongoDB")

      return data


    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def load_data_with_checking(self):
      return self.load_data()

    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def get_collection_min_max_dates(self, date_key: str):
      db = self.client[self.database_name]
      collection = db[self.collection_name]
      dates = [doc[date_key] for doc in collection.find({}, {date_key: 1})]

      if not dates:
          raise ValueError(f" Didn't find '{date_key}' in [Database: {self.database_name}], [Collection: {self.collection_name}]")
          # return None, None

      min_date = min(dates)
      max_date = max(dates)

      return min_date, max_date


    @MongoDBManager.ensure_database_exists
    def get_collection_list_from_database(self):
      db = self.client[self.database_name]
      collection_names = db.list_collection_names()

      collection_list = []

      # Check if there are any collections
      if collection_names:
          print(f"Found Collections in {self.database_name}")
          for collection_name in collection_names:
              collection_list.append(collection_name)
      else:
          print(f"No collections found in {self.database_name}")

      return collection_list

    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def get_unique_value(self, key: str) -> List[Any]:
        db = self.client[self.database_name]
        collection = db[self.collection_name]
        value_list = collection.distinct(key)
        return value_list

    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def find_items_by_key_value(self, key: str, value: Any) -> List[Any]:
        db = self.client[self.database_name]
        collection = db[self.collection_name]
        query = {key: value}
        items = list(collection.find(query))
        return items


    @MongoDBManager.ensure_database_exists
    @MongoDBManager.ensure_collection_exists
    def filter_collection_by_date(self, date_key: str, input_date: str, comparison_operator: str):
      db = self.client[self.database_name]
      collection = db[self.collection_name]

      # Define the filter query based on the comparison operator
      filter_query = {}
      if comparison_operator == '=':
          filter_query = {date_key: input_date}
      elif comparison_operator == '>':
          filter_query = {date_key: {'$gt': input_date}}
      elif comparison_operator == '<':
          filter_query = {date_key: {'$lt': input_date}}

      # Filter documents by date
      filtered_documents = collection.find(filter_query)

      return list(collection.find(filter_query))
