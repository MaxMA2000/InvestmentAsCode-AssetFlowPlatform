import requests
import os

from dotenv import load_dotenv

# Load the environment variables from the .env file
load_dotenv()

# Access the API key value from the environment
api_key = os.getenv('FMP_API_KEY')

api_url = f"https://financialmodelingprep.com/api/v3/stock/list?apikey={api_key}"

# Send GET request
response = requests.get(api_url)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Convert the response to a list of dictionaries
    data = response.json()
    # print(data)
else:
    print("Error occurred:", response.status_code)



from pymongo import MongoClient

# Create a MongoClient object and specify the connection URL
client = MongoClient('mongodb://localhost:27017/')


database_name = "ingestion_database"
collection_name = "stock_list"

# Access a specific database
db = client[database_name]

# Access a specific collection within the database
collection = db[collection_name]


# Insert each item in the "data" list into the collection
for item in data:
    collection.insert_one(item)
