from abc import ABC, abstractmethod

import pandas as pd
from pandas import DataFrame


class BookParser(ABC):
    @abstractmethod
    def parse_new_books(self) -> DataFrame:
        """
        Parse and returns new books from source.
        :return: data frame of books with schema:
        timestamp,
        store_id,
        title,
        author,
        isbn,
        description,
        rating,
        price,
        type_id.
        If no books are found it will return an empty data frame.
        """
        pass

    @abstractmethod
    def parse_popular_books(self) -> DataFrame:
        """
        Parse and returns popular books from source.
        :return: data frame of books with schema:
        timestamp,
        store_id,
        title,
        author,
        isbn,
        description,
        rating,
        price,
        type_id.
        If no books are found it will return an empty data frame.
        """
        pass

    @abstractmethod
    def parse_books_with_discount(self) -> DataFrame:
        """
        Parse and returns books with discount.
        :return: data frame of books with schema:
        timestamp,
        store_id,
        title,
        author,
        isbn,
        description,
        rating,
        price,
        type_id.
        If no books are found it will return an empty data frame.
        """
        pass

    def get_books_all_types(self) -> DataFrame:
        """
        Parse and returns new, popular and with discount books.
        :return: data frame of books with schema:
        timestamp,
        store_id,
        title,
        author,
        isbn,
        description,
        rating,
        price,
        type_id.
        If no books are found it will return an empty data frame.
        """
        return pd.concat(
            [
                self.parse_new_books(),
                self.parse_popular_books(),
                self.parse_books_with_discount(),
            ]
        ).reset_index(drop=True)
