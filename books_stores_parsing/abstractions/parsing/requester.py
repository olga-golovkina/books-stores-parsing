from abc import ABC, abstractmethod

from bs4 import BeautifulSoup
from yarl import URL


class Requester(ABC):
    @abstractmethod
    def request(self, url: URL) -> BeautifulSoup:
        pass
