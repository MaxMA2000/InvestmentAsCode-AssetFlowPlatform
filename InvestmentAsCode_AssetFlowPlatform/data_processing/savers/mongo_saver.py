import os
from pymongo import MongoClient
from typing import Dict, Any, List

from .saver import Saver
from InvestmentAsCode_AssetFlowPlatform.data_processing.managers.mongo_manager import MongoDBManager

from dotenv import load_dotenv
load_dotenv()

class MongoSaver(Saver):
    """Child class to save data to MongoDB"""

    def __init__(self, config: Dict[str, str]):
        super().__init__(config)

        self.database_name = config.get('database_name')
        self.collection_name = config.get('collection_name')

        self.manager = MongoDBManager()
        self.client = self.manager.connect_mongo_db()


    def save_data(self, data: List[Dict[str, Any]]) -> None:
        """save data into existing collection in database

        Args:
            data (List[Dict[str, Any]]): data to be stored, in a list of dictionaries format
        """
        db = self.client[self.database_name]
        collection = db[self.collection_name]

        # Insert each item into the collection
        print(f"Start writing data to MongoDB [Database: {self.database_name}], [Collection: {self.collection_name}]")
        collection.insert_many(data)
        print(f"Finish writing data to MongoDB")

    def replace_collection(self, data: List[Dict[str, Any]]) -> None:
        """replace the entire collection with input data

        Args:
            data (List[Dict[str, Any]]): data to be stored, in a list of dictionaries format
        """
        self.manager.remove_collection(self.client,
                                       self.database_name,
                                       self.collection_name)
        self.save_data(data)
