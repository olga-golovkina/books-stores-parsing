from pathlib import Path

import pandas as pd
from hydra import compose, initialize

from books_stores_parsing.telegram.bot_publisher import TelegramBotPublisher
from books_stores_parsing.telegram.post_creator import TelegramPostCreator
from books_stores_parsing.telegraph.article_creator import TelegraphHtmlArticleCreator
from books_stores_parsing.telegraph.article_publisher import TelegraphArticlePublisher


def publish():
    initialize(
        version_base=None, config_path="configs", job_name="books_stores_parsing"
    )

    categories = compose(config_name="book_categories")
    tg_config = compose(config_name="tg_config")
    store_ids = compose(config_name="store_ids")

    chat_id = tg_config["chat_id"]
    api_token = tg_config["bot_api_token"]

    tg_post_creator = TelegramPostCreator(
        TelegraphHtmlArticleCreator(Path("patterns")),
        TelegraphArticlePublisher(),
        store_ids,
        categories,
    )

    tg_publisher = TelegramBotPublisher(tg_post_creator, chat_id, api_token)

    books = pd.read_csv("./output/books.txt", sep=";")

    tg_publisher.publish(books)


# if __name__ == "__main__":
#     publish()
