from abc import ABC, abstractmethod

from pandas import DataFrame


class ArticleCreator(ABC):
    @abstractmethod
    def create_article(self, article_data: DataFrame):
        """
        Create info article by frame data
        :param article_data: data frame with analytical data
        :return: article body
        """
        pass
