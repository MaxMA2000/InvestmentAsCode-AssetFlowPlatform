import os
import requests
from loader import Loader
from dotenv import load_dotenv
from typing import Dict, Any

load_dotenv()

class ApiLoader(Loader):
    api_url: str
    __api_key: str
    parameters: Dict[str, Any]

    def __init__(self, config: Dict[str, Any]):
        super().__init__(config)
        self.api_url = config.get("api_url")
        self.__api_key = os.getenv('FMP_API_KEY')
        self.parameters = self._add_api_key(config.get("parameters"))

    def _add_api_key(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
        parameters["api_key"] = self.__api_key
        return parameters

    def fetch_data(self) -> Any:
        try:
            print(f"Sending GET API Requests to {self.api_url}")
            response = requests.get(self.api_url, self.parameters)

            print(f"Finish GET requests, Response Status Code = {response.status_code}")
            if response.status_code != 200:
                return f"Error: Unexpected response {response}"

            data = response.json()

            return data

        except requests.exceptions.HTTPError as error:
            return f"Error: Unexpected response {error}"

    def load_data(self, data: Any):
        pass
