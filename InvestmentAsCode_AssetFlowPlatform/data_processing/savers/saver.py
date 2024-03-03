from abc import ABC, abstractmethod

class Saver(ABC):
    """Interface class for Savers"""

    def __init__(self, config):
        # Initialize the saver with any necessary configuration
        self.config = config

    @abstractmethod
    def save_data(self, data):
        # Save the data to the desired destination
        raise NotImplementedError("Subclasses must implement save_data()")
