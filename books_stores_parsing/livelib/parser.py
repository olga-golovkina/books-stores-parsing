import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from pandas import DataFrame
from yarl import URL

from ..abstractions.parsing.book_parser import BookParser


class LivelibBookParser(BookParser):
    def __init__(self, book_selling_statuses: DictConfig, store_id: int):
        self.__domain = URL("https://www.livelib.ru/")

        self.__book_selling_statuses = book_selling_statuses
        self.__store_id = store_id

    @staticmethod
    def __get_soup_by_url(url: URL):
        return BeautifulSoup(requests.get(str(url)).text, "lxml")

    def __get_books_grid(self, page_soup: BeautifulSoup):
        return page_soup.find("ul", {"id": "books-more"})

    def __get_all_books_from_grid(self, grid_soup: BeautifulSoup):
        return grid_soup.findAll("li", {"class": "book-item__item "})

    def __get_title(self, book_page: BeautifulSoup) -> str:
        return book_page.find("h1", {"class": "bc__book-title "}).text.strip()

    def __get_author(self, book_page: BeautifulSoup) -> str:
        return (
            book_page.find("h2", {"class": "bc-author"})
            .find("a", {"class": "bc-author__link"})
            .text.strip()
        )

    def __get_isbn(self, book_page: BeautifulSoup) -> str:
        raw_isbn = (
            book_page.find("div", {"class": "bc-info"})
            .find("p", {"text": "ISBN: "})
            .find("span")
        )

        return raw_isbn.text.strip() if raw_isbn is not None else None

    def __get_description(self, book_page: BeautifulSoup) -> str:
        return book_page.find(
            "div", {"id": "lenta-card__text-edition-full"}
        ).text.replace("<br>", "\n")

    def __get_rating(self, book_page: BeautifulSoup) -> float:
        return float(
            book_page.find("div", {"class": "bc-rating"})
            .find("span")
            .text.strip()
            .replace(",", ".")
        )

    def __get_price(self, book_page: BeautifulSoup) -> float:
        price_panel = book_page.find("div", {"class": "lightlabel-book24-card"})

        if price_panel is None:
            return 0.0

        return float(
            re.findall(r"\d+", price_panel.find("p", {"class": "price"}).text)[0]
        )

    def __get_url(self, book_soup: BeautifulSoup) -> URL:
        return self.__domain.with_path(
            book_soup.find("a", {"class": "book-item__title"}, href=True)["href"]
        )

    def __extract_book_data(self, raw_book_data) -> dict:
        book_url = self.__get_url(raw_book_data)
        book_page = self.__get_soup_by_url(book_url)

        return {
            "timestamp": datetime.now().timestamp(),
            "store_id": self.__store_id,
            "title": self.__get_title(book_page),
            "author": self.__get_author(book_page),
            "isbn": self.__get_isbn(book_page),
            "description": self.__get_description(book_page),
            "rating": self.__get_rating(book_page),
            "price": self.__get_price(book_page),
        }

    def __extract_books(self, main_page: BeautifulSoup) -> DataFrame:
        books_grid = self.__get_books_grid(main_page)
        books_raw = self.__get_all_books_from_grid(books_grid)

        books = map(self.__extract_book_data, books_raw)

        return DataFrame(books)

    def parse_new_books(self) -> DataFrame:
        new_page = self.__get_soup_by_url(self.__domain.with_path("books/novelties"))

        books_df = self.__extract_books(new_page)
        books_df["category_id"] = self.__book_selling_statuses["new"]

        return books_df

    def parse_popular_books(self) -> DataFrame:
        popular_page = self.__get_soup_by_url(self.__domain.with_path("popular"))

        books_df = self.__extract_books(popular_page)
        books_df["category_id"] = self.__book_selling_statuses["popular"]

        return books_df

    def parse_books_with_discount(self) -> DataFrame:
        return DataFrame(
            columns=[
                "timestamp",
                "store_id",
                "title",
                "author",
                "isbn",
                "description",
                "rating",
                "price",
                "category_id",
            ]
        )
