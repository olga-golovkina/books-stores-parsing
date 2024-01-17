import datetime

from omegaconf import DictConfig
from pandas import DataFrame

from ..abstractions.creators.article_creator import ArticleCreator
from ..abstractions.creators.post_creator import PostCreator
from ..abstractions.publishing.article_publisher import ArticlePublisher


class TelegramPostCreator(PostCreator):
    def __init__(
        self,
        article_creator: ArticleCreator,
        article_publisher: ArticlePublisher,
        stores_cfg: DictConfig,
        categories: DictConfig,
    ):
        self.__categories = categories
        self.__article_creator = article_creator
        self.__article_publisher = article_publisher
        self.__store_cfg = stores_cfg

    def __create_title(self, date_now: datetime):
        return "**Ежедневная сводка от {date} по книгам в магазинах**".format(
            date=date_now.date().strftime("%d\\.%m\\.%Y")
        )

    def __create_store_block(self, date_now: datetime, store: str, books: DataFrame):

        store_id = self.__store_cfg[store]

        books_of_store = books[books["store_id"] == store_id]
        books_count = len(books_of_store.index)

        if books_count == 0:
            return None

        content = f"**{store.upper()}:**"

        for category_key in self.__categories:
            category = self.__categories[category_key]
            category_id = category["id"]
            category_name = category["name"]

            books_by_category = books_of_store[
                books_of_store["category_id"] == category_id
            ]
            books_count = len(books_by_category.index)

            if books_count == 0:
                continue

            article_title = f"{category_name} на {store} от {date_now.date()}"
            article_content = self.__article_creator.create_article(
                books_by_category.head(10)
            )
            article_url = self.__article_publisher.publish(
                article_title, article_content
            )

            content += f"""
[{category_name}]({article_url})"""

        return content.strip()

    def create(self, books_post_data: DataFrame):
        date_now = datetime.datetime.now()

        title = self.__create_title(date_now)
        content = (
            """
    """
            * 2
        ).join(
            filter(
                lambda x: x is not None,
                [
                    self.__create_store_block(date_now, store, books_post_data)
                    for store in self.__store_cfg.keys()
                ],
            )
        )

        return (
            title
            + (
                """
        """
                * 2
            )
            + content
        )
