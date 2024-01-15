from pathlib import Path

from pandas import DataFrame

from books_stores_parsing.abstractions.creators.article_creator import ArticleCreator


class TelegraphHtmlArticleCreator(ArticleCreator):
    def __init__(self, patterns_folder: Path):
        with patterns_folder.joinpath("book_card_pattern.txt").open(
            encoding="utf-8"
        ) as file:
            self.__book_card_pattern = file.read()

    def __create_book_card(self, book):
        return self.__book_card_pattern.format(
            title=book["title"],
            author=book["author"],
            isbn=book["isbn"],
            rating=book["rating"],
            price=book["price"],
            url=book["url"],
            img_url=book["img_url"],
            description=book["description"],
        )

    def create_article(self, article_data: DataFrame):
        return "\n".join(map(self.__create_book_card, article_data.to_dict("records")))
