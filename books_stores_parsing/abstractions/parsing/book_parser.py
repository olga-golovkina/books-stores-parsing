from abc import ABC, abstractmethod

from pandas import DataFrame


class BookParser(ABC):

    @abstractmethod
    def parse_new_books(self) -> DataFrame:
        """
        Parse and returns new books from source.
        :return: data frame of books with schema:
        timestamp, store, title, authors, publisher, description, status_id, rating, price.
        If no books are found it will return an empty data frame.
        """
        pass

    @abstractmethod
    def parse_popular_books(self) -> DataFrame:
        """
        Parse and returns popular books from source.
        :return: data frame of books with schema:
        timestamp, store, title, authors, publisher, description, status_id, rating, price.
        If no books are found it will return an empty data frame.
        """
        pass

    @abstractmethod
    def parse_books_with_discount(self) -> DataFrame:
        """
        Parse and returns books with discount.
        :return: data frame of books with schema:
        timestamp, store, title, authors, publisher, description, status_id, rating, price.
        If no books are found it will return an empty data frame.
        """
        pass
