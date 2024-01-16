from abc import ABC, abstractmethod

from pandas import DataFrame


class ArticleCreator(ABC):
    @abstractmethod
    def create_article(self, article_data: DataFrame):
        """
        Create info article by frame output
        :param article_data: output frame with analytical output
        :return: article body
        """
        pass
