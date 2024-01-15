from abc import ABC, abstractmethod

from yarl import URL


class ArticlePublisher(ABC):
    @abstractmethod
    def publish(self, title: str, article_data) -> URL:
        """
        Publish data to article
        :param title: the title of article which use on creating article URL
        :param article_data: html code which convert to article
        :return: url to created article. If article didn't create, return None
        """
        pass
