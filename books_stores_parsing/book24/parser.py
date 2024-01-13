from datetime import datetime
from typing import List

import requests
from bs4 import BeautifulSoup
from omegaconf import DictConfig
from pandas import DataFrame
from yarl import URL

from ..abstractions.parsing.book_parser import BookParser


class Book24BookParser(BookParser):
    def __init__(self, book_selling_statuses: DictConfig, store_id: int):
        self.__domain = URL("https://book24.ru/")

        self.__book_selling_statuses = book_selling_statuses
        self.__store_id = store_id

    @staticmethod
    def __get_soup_by_url(url: URL):
        return BeautifulSoup(requests.get(str(url)).text, "lxml")

    def __get_books_grid(self, page_soup: BeautifulSoup):
        return page_soup.find("div", {"class": "catalog-page__body"}).find(
            "div", {"class": "product-list catalog__product-list"}
        )

    def __get_all_books_from_grid(self, grid_soup: BeautifulSoup):
        return grid_soup.findAll("div", {"class": "product-list__item"})

    def __get_title(self, book_page: BeautifulSoup) -> str:
        return book_page.find(
            "h1", {"class": "product-detail-page__title"}
        ).text.strip()

    def __is_author_field(self, book_properties: BeautifulSoup) -> bool:
        return (
            "автор"
            in book_properties.find(
                "dt", {"class": "product-characteristic__label-holder"}
            ).text.lower()
        )

    def __get_author(self, book_properties: List[BeautifulSoup]) -> str:
        return (
            next(filter(self.__is_author_field, book_properties))
            .find("dd", {"class": "product-characteristic__value"})
            .text
        )

    def __is_isbn_field(self, book_properties: List[BeautifulSoup]) -> bool:
        return (
            "isbn"
            in book_properties.find(
                "dt", {"class": "product-characteristic__label-holder"}
            ).text.lower()
        )

    def __get_isbn(self, book_properties: List[BeautifulSoup]) -> str:
        isbn_field = next(filter(self.__is_isbn_field, book_properties))

        return isbn_field.text if isbn_field is not None else None

    def __extract_description_text(self, raw_desc: BeautifulSoup) -> str:
        return raw_desc.text.strip()

    def __get_description(self, book_page: BeautifulSoup) -> str:
        return "\n".join(
            map(
                self.__extract_description_text,
                book_page.find("div", {"class": "product-about__text"}).findAll("p"),
            )
        )

    def __get_rating(self, book_page: BeautifulSoup) -> float:
        return float(
            book_page.find("span", {"class": "rating-widget__main-text"})
            .text.strip()
            .replace(",", ".")
        )

    def __get_price(self, book_page: BeautifulSoup) -> float:
        price_block = book_page.find("meta", {"itemprop": "price"})

        return float(price_block["content"]) if price_block is not None else 0.0

    def __get_url(self, book_soup: BeautifulSoup) -> URL:
        return self.__domain.with_path(
            book_soup.find("a", {"class": "product-card__name"}, href=True)["href"]
        )

    def __get_full_properties(self, book_page: BeautifulSoup):
        return book_page.find(
            "div",
            {
                "class": "product-characteristic "
                "product-detail-page__product-characteristic"
            },
        ).findAll("div", {"class": "product-characteristic__item"})

    def __extract_book_data(self, raw_book_data) -> dict:
        book_url = self.__get_url(raw_book_data)
        book_page = self.__get_soup_by_url(book_url)

        book_props = self.__get_full_properties(book_page)

        return {
            "timestamp": datetime.now().timestamp(),
            "store_id": self.__store_id,
            "url": str(book_url),
            "title": self.__get_title(book_page),
            "author": self.__get_author(book_props),
            "isbn": self.__get_isbn(book_props),
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
        new_page = self.__get_soup_by_url(self.__domain.with_path("novie-knigi"))

        books_df = self.__extract_books(new_page)
        books_df["category_id"] = self.__book_selling_statuses["new"]

        return books_df

    def parse_popular_books(self) -> DataFrame:
        popular_page = self.__get_soup_by_url(
            self.__domain.with_path("knigi-bestsellery")
        )

        books_df = self.__extract_books(popular_page)
        books_df["category_id"] = self.__book_selling_statuses["popular"]

        return books_df

    def parse_books_with_discount(self) -> DataFrame:
        discount_page = self.__get_soup_by_url(self.__domain.with_path("best-price"))

        books_df = self.__extract_books(discount_page)
        books_df["category_id"] = self.__book_selling_statuses["discounted"]

        return books_df
