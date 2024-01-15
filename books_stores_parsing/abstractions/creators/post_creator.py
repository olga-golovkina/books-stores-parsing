from abc import ABC, abstractmethod

from pandas import DataFrame


class PostCreator(ABC):
    @abstractmethod
    def create(self, books_post_data: DataFrame):
        pass
