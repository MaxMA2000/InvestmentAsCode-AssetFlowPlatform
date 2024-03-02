import os
import requests
from loader import Loader
from dotenv import load_dotenv

load_dotenv()

class ApiLoader(Loader):

  def __init__(self, config):
      super().__init__(config)
      self.api_url = config.get("api_url")
      self.__api_key = os.getenv('FMP_API_KEY')
      self.parameters = self._add_api_key(config.get("parameters"))

  def _add_api_key(self, parameters):
    parameters["api_key"] = self.__api_key
    return parameters

  def fetch_data(self):
    try:
      print(f"Sending GET API Requests to {self.api_url}")
      response = requests.get(self.api_url, self.parameters)

      print(f"Finish GET requests, Reponse Status Code = {response.status_code}")
      if response.status_code != 200:
        return f"Error: Unexpected response {response}"

      data = response.json()

      return data

    except requests.exceptions.HTTPError as error:
        return f"Error: Unexpected response {error}"

  def load_data(self, data):
      pass
