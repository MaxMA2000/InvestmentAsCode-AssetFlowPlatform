import os
from .loader import Loader
from pymongo import MongoClient
from typing import Dict, Any, Callable

class MongoLoader(Loader):

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.database_name = config.get('database_name')
        self.collection_name = config.get('collection_name')
        self.mongo_server_url = self._add_mongo_server_url()
        self.client = self._connect_mongodb()

    @staticmethod
    def _add_mongo_server_url() -> str:
        return os.getenv('MONGO_SERVER_URL')


    def close_mongodb_connection(self):
      self.client.close()
      print(f"Successfully Stop connecting with MongoDB at {self.mongo_server_url}")
      self.client = None

    def _connect_mongodb(self):
      try:
        client = MongoClient(self.mongo_server_url)
        print(f"Successfully connected with MongoDB at {self.mongo_server_url}")
        return client
      except:
        error_message = f"Having Error Connecting MongoDB at {self.mongo_server_url}"
        raise ValueError(error_message)

    def check_database_exists(self):
      try:
        if self.database_name in self.client.list_database_names():
            print(f"The database '{self.database_name}' exists.")
            return True
        else:
            print(f"The database '{self.database_name}' does not exist.")
            return False
      except Exception as e:
          print(f"An error occurred while checking database existence: {str(e)}")
          return False

    def ensure_database_exists(func):
        def wrapper(self, *args, **kwargs):
            if self.check_database_exists():
                return func(self, *args, **kwargs)
            else:
                print(f"The database '{self.database_name}' does not exist.")
                error_message = f"The database '{self.database_name}' does not exist."
                raise ValueError(error_message)
        return wrapper

    @ensure_database_exists
    def check_collection_exists(self):
      try:
          database = self.client[self.database_name]
          collection_names = database.list_collection_names()
          if self.collection_name in collection_names:
              print(f"The collection '{self.collection_name}' exists in the '{self.database_name}' database.")
              return True
          else:
              print(f"The collection '{self.collection_name}' does not exist in the '{self.database_name}' database.")
              return False
      except Exception as e:
          print(f"An error occurred while checking collection existence: {str(e)}")
          return False

    def ensure_collection_exists(func):
      def wrapper(self, *args, **kwargs):
          if self.check_collection_exists():
              return func(self, *args, **kwargs)
          else:
              print(f"The collection '{self.collection_name}' does not exist in the '{self.database_name}' database.")
              error_message = f"The collection '{self.collection_name}' does not exist in the '{self.database_name}' database."
              raise ValueError(error_message)
      return wrapper

    def fetch_data(self):
      pass

    @ensure_collection_exists
    def load_data(self):
      db = self.client[self.database_name]
      collection = db[self.collection_name]

      # Load data from MongoDB
      print("Start loading data from MongoDB")
      data = list(collection.find())
      print("Finish loading data from MongoDB")

      return data
