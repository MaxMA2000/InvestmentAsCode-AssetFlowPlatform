import os
from dotenv import load_dotenv
from typing import Dict, Any, Callable
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from pymongo.pool import Pool

load_dotenv()


class MongoDBManager:
    _pool: Pool = None
    _mongo_server_url: str = str(os.getenv("MONGO_SERVER_URL"))
    _max_pool_size: int = int(os.getenv("MAX_POOL_SIZE"))

    @classmethod
    def initialize_pool(cls):
        cls._pool = MongoClient(cls._mongo_server_url, maxPoolSize=cls._max_pool_size)

    @classmethod
    def acquire_connection(cls):
        if not cls._pool:
            raise ConnectionFailure("The MongoDB connection pool is not initialized.")

        return cls._pool

    @classmethod
    def connect_mongo_db(cls):
        cls.initialize_pool()
        return cls.acquire_connection()

    @classmethod
    def release_connection(cls, connection):
        if connection:
            connection.close()

    @staticmethod
    def check_database_exists(client, database_name):
        try:
            if database_name in client.list_database_names():
                print(f"The database '{database_name}' exists.")
                return True
            else:
                print(f"The database '{database_name}' does not exist.")
                return False
        except Exception as e:
            print(f"An error occurred while checking database existence: {str(e)}")
            return False

    @staticmethod
    def check_collection_exists(client, database_name: str, collection_name: str):
        try:
            database = client[database_name]
            collection_names = database.list_collection_names()
            if collection_name in collection_names:
                print(
                    f"The collection '{collection_name}' exists in the '{database_name}' database."
                )
                return True
            else:
                print(
                    f"The collection '{collection_name}' does not exist in the '{database_name}' database."
                )
                return False
        except Exception as e:
            print(f"An error occurred while checking collection existence: {str(e)}")
            return False

    @classmethod
    def ensure_database_exists(cls, func: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            if self.manager.check_database_exists(self.client, self.database_name):
                return func(self, *args, **kwargs)
            else:
                print(f"The database '{self.database_name}' does not exist.")
                error_message = f"The database '{self.database_name}' does not exist."
                raise ValueError(error_message)

        return wrapper

    @classmethod
    def ensure_collection_exists(cls, func: Callable) -> Callable:
        def wrapper(self, *args, **kwargs):
            if self.manager.check_collection_exists(
                self.client, self.database_name, self.collection_name
            ):
                return func(self, *args, **kwargs)
            else:
                print(
                    f"The collection '{self.collection_name}' does not exist in the '{self.database_name}' database."
                )
                error_message = f"The collection '{self.collection_name}' does not exist in the '{self.database_name}' database."
                raise ValueError(error_message)

        return wrapper

    def remove_collection(self, config: Dict[str, str]):
        connection = self.acquire_connection()
        database_name = config.get("database_name")
        collection_name = config.get("collection_name")

        # Connect to MongoDB
        db = connection[database_name]
        collection = db[collection_name]

        # Remove the collection
        collection.drop()
        print(
            f"The collection '{collection_name}' has been removed from the '{database_name}' database."
        )

        # Release the connection
        self.release_connection(connection)
