import os
import requests
from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()

api_key = os.getenv('FMP_API_KEY')
mongo_server_url = os.getenv('MONGO_SERVER_URL')

api_url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={api_key}"

database_name = "ingestion_database"
collection_name = "stock_list"


def send_get_request():
  try:
    # Send GET request to API
    response = requests.get(api_url)

    if response.status_code != 200:
       return f"Error: Unexpected response {response}"

    data = response.json()

    return data

  except requests.exceptions.HTTPError as error:
      return f"Error: Unexpected response {error}"


data = send_get_request()


if type(data) == list:

    # Create Mongo Client and connect with MongoDB Server
    client = MongoClient(mongo_server_url)

    db = client[database_name]
    collection = db[collection_name]

    # Insert each item into the collection
    for item in data:
        collection.insert_one(item)



