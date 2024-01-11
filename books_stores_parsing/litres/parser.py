from pandas import DataFrame

from ..abstractions.parsing.book_parser import BookParser


class LitresBookParser(BookParser):
    def parse_new_books(self) -> DataFrame:
        pass

    def parse_popular_books(self) -> DataFrame:
        pass

    def parse_books_with_discount(self) -> DataFrame:
        pass
