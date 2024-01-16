from datetime import datetime
from typing import List

from bs4 import BeautifulSoup
from omegaconf import DictConfig
from pandas import DataFrame
from yarl import URL

from ..abstractions.parsing.book_parser import BookParser
from ..abstractions.parsing.requester import Requester


class Book24BookParser(BookParser):
    def __init__(self, requester: Requester, categories: DictConfig, store_id: int):
        self.__requester = requester

        self.__domain = URL("https://book24.ru/")

        self.__categories = categories
        self.__store_id = store_id

    def __get_books_grid(self, page_soup: BeautifulSoup):
        return page_soup.find("div", {"class": "catalog-page__body"}).find(
            "div", {"class": "product-list catalog__product-list"}
        )

    def __get_all_books_from_grid(self, grid_soup: BeautifulSoup):
        return grid_soup.findAll("div", {"class": "product-list__item"})

    def __get_title(self, book_page: BeautifulSoup) -> str:
        return (
            book_page.find("h1", {"class": "product-detail-page__title"})
            .text.split(":")[-1]
            .strip()
        )

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

        return (
            isbn_field.text.replace("ISBN:", "").strip()
            if isbn_field is not None
            else None
        )

    def __extract_description_text(self, raw_desc: BeautifulSoup) -> str:
        return raw_desc.text.strip()

    def __get_description(self, book_page: BeautifulSoup) -> str:

        desc_block = book_page.find("div", {"class": "product-about__text"})

        if desc_block is None:
            return "Нет описания"

        return "<br>".join(
            map(
                self.__extract_description_text,
                desc_block.findAll("p"),
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

    def __get_img_url(self, book_page: BeautifulSoup) -> str:
        return book_page.find(
            "picture", {"class": "product-poster__main-picture"}
        ).find("img", {"class": "product-poster__main-image"})["src"]

    def __extract_book_data(self, raw_book_data) -> dict:
        book_url = self.__get_url(raw_book_data)
        book_page = self.__requester.request(book_url)

        book_props = self.__get_full_properties(book_page)

        return {
            "timestamp": datetime.now().timestamp(),
            "store_id": self.__store_id,
            "url": str(book_url),
            "title": self.__get_title(book_page),
            "img_url": self.__get_img_url(book_page),
            "author": self.__get_author(book_props),
            "isbn": self.__get_isbn(book_props),
            "description": self.__get_description(book_page),
            "rating": self.__get_rating(book_page),
            "price": self.__get_price(book_page),
        }

    def __extract_books(self, main_page: BeautifulSoup) -> DataFrame:
        books_grid = self.__get_books_grid(main_page)

        if books_grid is None:
            return BookParser._create_empty_books()

        books_raw = self.__get_all_books_from_grid(books_grid)

        books = map(self.__extract_book_data, books_raw)

        return DataFrame(books)

    def parse_new_books(self) -> DataFrame:
        new_page = self.__requester.request(self.__domain.with_path("novie-knigi"))

        books_df = self.__extract_books(new_page)
        books_df["category_id"] = self.__categories["new"]["id"]

        return books_df

    def parse_popular_books(self) -> DataFrame:
        popular_page = self.__requester.request(
            self.__domain.with_path("knigi-bestsellery")
        )

        books_df = self.__extract_books(popular_page)
        books_df["category_id"] = self.__categories["popular"]["id"]

        return books_df

    def parse_books_with_discount(self) -> DataFrame:
        discount_page = self.__requester.request(self.__domain.with_path("best-price"))

        books_df = self.__extract_books(discount_page)
        books_df["category_id"] = self.__categories["discounted"]["id"]

        return books_df
