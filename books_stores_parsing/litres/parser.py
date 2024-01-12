import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from pandas import DataFrame
from yarl import URL

from ..abstractions.parsing.book_parser import BookParser


class LitresBookParser(BookParser):
    def __init__(self, book_selling_statuses: DictConfig):
        self.__domain = URL("https://www.litres.ru/")
        self.__book_selling_statuses = book_selling_statuses

    @staticmethod
    def __get_soup_by_url(url: URL):
        return BeautifulSoup(requests.get(str(url)).text)

    def __get_books_grid(self, page_soup: BeautifulSoup):
        return page_soup.find(
            "div", {"class": "ArtsGrid-module__artsGrid__wrapper_2s8A9"}
        )

    def __get_all_books_from_grid(self, grid_soup: BeautifulSoup):
        return grid_soup.findAll("div", {"class": "ArtsGrid-module__artWrapper_1j1xJ"})

    def __get_title(self, book_page: BeautifulSoup) -> str:
        return book_page.find("h1", {"itemprop": "name"}).text.strip()

    def __get_author(self, book_page: BeautifulSoup) -> str:
        return (
            book_page.find("div", {"class": "Authors-module__authors__wrapper_1rZey"})
            .find("span", {"itemprop": "name"})
            .text.strip()
        )

    def __get_isbn(self, book_page: BeautifulSoup) -> str:
        raw_isbn = book_page.find("span", {"itemprop": "isbn"})

        return raw_isbn.text.strip() if raw_isbn is not None else None

    def __extract_description_text(self, raw_desc: BeautifulSoup) -> str:
        return raw_desc.text.strip()

    def __get_description(self, book_page: BeautifulSoup) -> str:
        return "\n".join(
            map(
                self.__extract_description_text,
                book_page.find("div", {"itemprop": "description"}).findAll("p"),
            )
        )

    def __get_rating(self, book_page: BeautifulSoup) -> float:
        return float(book_page.find("meta", {"itemprop": "ratingValue"})["content"])

    def __get_price(self, book_page: BeautifulSoup) -> float:
        price_block = book_page.find(
            "strong", {"class": "SaleBlock-module__block__price__default_kE68R"}
        )

        return (
            re.findall(r"\d+", price_block.text)[0] if price_block is not None else 0.0
        )

    def __get_url(self, book_soup: BeautifulSoup) -> URL:
        return self.__domain.with_path(
            book_soup.find("a", {"data-test-id": "art__title--desktop"}, href=True)[
                "href"
            ]
        )

    def __extract_book_data(self, raw_book_data) -> dict:
        book_url = self.__get_url(raw_book_data)
        book_page = self.__get_soup_by_url(book_url)

        return {
            "timestamp": datetime.now().timestamp(),
            "store": "litres.ru",
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
        new_page = self.__get_soup_by_url(self.__domain.with_path("new"))

        books_df = self.__extract_books(new_page)
        books_df["type_id"] = self.__book_selling_statuses["new"]

        return books_df

    def parse_popular_books(self) -> DataFrame:
        popular_page = self.__get_soup_by_url(self.__domain.with_path("popular"))

        books_df = self.__extract_books(popular_page)
        books_df["type_id"] = self.__book_selling_statuses["popular"]

        return books_df

    def parse_books_with_discount(self) -> DataFrame:
        discount_page = self.__get_soup_by_url(
            self.__domain.with_path("collections/samye-bolshie-skidki-segodnya")
        )

        books_df = self.__extract_books(discount_page)
        books_df["type_id"] = self.__book_selling_statuses["discounted"]

        return books_df
