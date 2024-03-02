from abc import ABC, abstractmethod

class Loader(ABC):
  """interface class for Loaders
  """
  def __init__(self, config):
      # Initialize the loader with any necessary configuration
      self.config = config

  @abstractmethod
  def fetch_data(self):
      # Fetch data from the data source
      raise NotImplementedError("Subclasses must implement fetch_data()")

  @abstractmethod
  def load_data(self, data):
      # Load and transform the fetched data
      raise NotImplementedError("Subclasses must implement load_data()")
