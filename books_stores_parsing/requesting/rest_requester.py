import time

import requests
from bs4 import BeautifulSoup
from yarl import URL

from ..abstractions.parsing.requester import Requester


class RestRequester(Requester):
    def __init__(self):
        self.__header = {
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/74.0.3729.169 "
            "Safari/537.36",
            "referer": "https://www.google.com/",
        }

        self.__session = requests.session()
        self.__session.cookies.clear()

    def request(self, url: URL) -> BeautifulSoup:
        time.sleep(2)

        response = self.__session.get(str(url), headers=self.__header)
        self.__session.cookies.clear()

        return BeautifulSoup(response.text, "lxml")
