import os
from pymongo import MongoClient
from .saver import Saver
from dotenv import load_dotenv
from typing import Dict, Any, List

class MongoSaver(Saver):
    """Child class to save data to MongoDB"""

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)
        self.database_name = config.get('database_name')
        self.collection_name = config.get('collection_name')
        self.mongo_server_url = self._add_mongo_server_url()

    @staticmethod
    def _add_mongo_server_url() -> str:
        return os.getenv('MONGO_SERVER_URL')

    def save_data(self, data: List[Dict[str, Any]]):
        # Create a MongoDB client and connect to the MongoDB server
        client = MongoClient(self.mongo_server_url)

        # Access the specified database and collection
        db = client[self.database_name]
        collection = db[self.collection_name]

        # Insert each item into the collection
        print(f"Start writing data to MongoDB...")
        collection.insert_many(data)
        print(f"Finish writing data to MongoDB")
