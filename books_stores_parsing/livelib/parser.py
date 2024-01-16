import re
from datetime import datetime

from bs4 import BeautifulSoup
from omegaconf import DictConfig
from pandas import DataFrame
from yarl import URL

from ..abstractions.parsing.book_parser import BookParser
from ..abstractions.parsing.requester import Requester


class LivelibBookParser(BookParser):
    def __init__(self, requester: Requester, categories: DictConfig, store_id: int):
        self.__requester = requester

        self.__domain = URL("https://www.livelib.ru/")

        self.__categories = categories
        self.__store_id = store_id

    def __get_books_grid(self, page_soup: BeautifulSoup):
        return page_soup.find("ul", {"id": "books-more"})

    def __get_all_books_from_grid(self, grid_soup: BeautifulSoup):
        return grid_soup.findAll("li", {"class": "book-item__item"})

    def __get_title(self, book_page: BeautifulSoup) -> str:
        return book_page.find("h1", {"class": "bc__book-title"}).text.strip()

    def __get_author(self, book_page: BeautifulSoup) -> str:
        return (
            book_page.find("h2", {"class": "bc-author"})
            .find("a", {"class": "bc-author__link"})
            .text.strip()
        )

    def __is_isbn(self, content):
        return "ISBN" in content

    def __get_isbn(self, book_page: BeautifulSoup) -> str:

        desc_block = book_page.find("div", {"class": "bc-info"})

        if desc_block is None:
            return ""

        isbn = filter(self.__is_isbn, desc_block.findAll("p"))

        if len(list(isbn)) == 0:
            return ""

        raw_isbn = next(isbn)

        return raw_isbn.text.strip() if raw_isbn is not None else ""

    def __get_description(self, book_page: BeautifulSoup) -> str:

        desc_block = book_page.find("div", {"id": "lenta-card__text-edition-full"})

        if desc_block is None:
            return "Нет описания"

        return desc_block.text.replace("<br>", "\n")

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
            book_soup.find("a", {"class": "book-item__link"}, href=True)["href"].split(
                "?"
            )[0]
        )

    def __get_img_url(self, book_page: BeautifulSoup) -> str:
        return book_page.find("img", {"id": "main-image-book"})["src"]

    def __extract_book_data(self, raw_book_data) -> dict:
        book_url = self.__get_url(raw_book_data)
        book_page = self.__requester.request(book_url)

        _ = book_page

        return {
            "timestamp": datetime.now().timestamp(),
            "store_id": self.__store_id,
            "title": self.__get_title(book_page),
            "img_url": self.__get_img_url(book_page),
            "author": self.__get_author(book_page),
            "isbn": self.__get_isbn(book_page),
            "description": self.__get_description(book_page),
            "rating": self.__get_rating(book_page),
            "price": self.__get_price(book_page),
        }

    def __extract_books(self, main_page: BeautifulSoup) -> DataFrame:
        books_grid = self.__get_books_grid(main_page)
        books_raw = self.__get_all_books_from_grid(books_grid)

        books = [self.__extract_book_data(book) for book in books_raw]

        return DataFrame(books)

    def parse_new_books(self) -> DataFrame:
        new_page = self.__requester.request(self.__domain.with_path("books/novelties"))

        books_df = self.__extract_books(new_page)
        books_df["category_id"] = self.__categories["new"]["id"]

        return books_df

    def parse_popular_books(self) -> DataFrame:
        popular_page = self.__requester.request(
            self.__domain.with_path("books/movers-and-shakers")
        )

        books_df = self.__extract_books(popular_page)
        books_df["category_id"] = self.__categories["popular"]["id"]

        return books_df

    def parse_books_with_discount(self) -> DataFrame:
        return DataFrame(
            columns=[
                "timestamp",
                "store_id",
                "url",
                "title",
                "img_url",
                "author",
                "isbn",
                "description",
                "rating",
                "price",
                "category_id",
            ]
        )
