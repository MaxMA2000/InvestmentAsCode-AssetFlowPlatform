import os
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.pool import Pool
from typing import Dict

load_dotenv()

class MongoDBManager:
    _pool: Pool = None
    _mongo_server_url: str = str(os.getenv('MONGO_SERVER_URL'))
    _max_pool_size: int = int(os.getenv('MAX_POOL_SIZE'))

    @classmethod
    def initialize_pool(cls):
        cls._pool = MongoClient(cls._mongo_server_url, maxPoolSize=cls._max_pool_size)

    @classmethod
    def acquire_connection(cls):
        if not cls._pool:
            raise ConnectionFailure("The MongoDB connection pool is not initialized.")

        return cls._pool

    @classmethod
    def release_connection(cls, connection):
        if connection:
            connection.close()

    def remove_collection(self, config: Dict[str, str]):
        connection = self.acquire_connection()
        database_name = config.get('database_name')
        collection_name = config.get('collection_name')

        # Connect to MongoDB
        db = connection[database_name]
        collection = db[collection_name]

        # Remove the collection
        collection.drop()
        print(f"The collection '{collection_name}' has been removed from the '{database_name}' database.")

        # Release the connection
        self.release_connection(connection)
