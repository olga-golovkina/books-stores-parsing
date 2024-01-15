from telegraph import Telegraph
from yarl import URL

from books_stores_parsing.abstractions.publishing.article_publisher import (
    ArticlePublisher,
)


class TelegraphArticlePublisher(ArticlePublisher):
    def __init__(self):
        self.__telegraph = Telegraph()
        self.__telegraph.create_account(
            short_name="book_store_publisher", author_name="Book Store Publisher Bot"
        )

    def publish(self, title: str, article_content) -> URL:
        return URL(
            self.__telegraph.create_page(title, html_content=article_content)["url"]
        )
