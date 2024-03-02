from loader import Loader

class ApiLoader(Loader):
    def __init__(self, config):
        super().__init__(config)
        self.api_url = config.get("api_url")
        self.parameters = config.get("parameters")

    def fetch_data(self):
        # Implement the logic to fetch data from the API using self.api_url and self.parameters
        pass

    def load_data(self, data):
        # Implement the logic to load and transform the fetched data
        pass
