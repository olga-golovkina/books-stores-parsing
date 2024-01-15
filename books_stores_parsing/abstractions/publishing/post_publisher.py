from abc import ABC, abstractmethod

from pandas import DataFrame


class PostPublisher(ABC):
    @abstractmethod
    def publish(self, publishing_data: DataFrame):
        pass
