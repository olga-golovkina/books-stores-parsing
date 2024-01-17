# from pathlib import Path
from cffi.model import StructType
from hydra import compose, initialize
from hydra.core.global_hydra import GlobalHydra
from pandas import DataFrame
from pyspark.sql import SparkSession

# from books_stores_parsing.telegram.bot_publisher import TelegramBotPublisher
# from books_stores_parsing.telegram.post_creator import TelegramPostCreator
# from books_stores_parsing.telegraph.article_creator import TelegraphHtmlArticleCreator
# from books_stores_parsing.telegraph.article_publisher import TelegraphArticlePublisher


def create_schema():
    return (
        StructType()
        .add("datetime", "datetime")
        .add("store_id", "integer")
        .add("url", "string")
        .add("title", "string")
        .add("img_url", "string")
        .add("isbn", "string")
        .add("description", "string")
        .add("rating", "float")
        .add("price", "integer")
        .add("category_id", "integer")
    )


def read_books() -> DataFrame:
    spark_session = (
        SparkSession.builder.master("yarn").appName("book_stores_parsing").getOrCreate()
    )

    path_cfg = compose(config_name="path_config")

    print("Hadoop path:")
    print(path_cfg["hadoop_books"])

    books = (
        spark_session.read.option("header", "True")
        .option("delimiter", ";")
        .csv(path_cfg["hadoop_books"], sep=";", schema=create_schema())
    )

    books.show(2)

    books = books.toPandas()

    print(books.head(2))

    print(books.info())

    return books


def publish():
    if not GlobalHydra.instance().is_initialized():
        initialize(
            version_base=None,
            config_path="../configs",
            job_name="books_stores_parsing",
        )

    # categories = compose(config_name="book_categories")
    # tg_config = compose(config_name="tg_config")
    # store_ids = compose(config_name="store_ids")
    #
    # chat_id = tg_config["chat_id"]
    # api_token = tg_config["bot_api_token"]
    #
    # tg_post_creator = TelegramPostCreator(
    #     TelegraphHtmlArticleCreator(Path("patterns")),
    #     TelegraphArticlePublisher(),
    #     store_ids,
    #     categories,
    # )

    # tg_publisher = TelegramBotPublisher(tg_post_creator, chat_id, api_token)

    books = read_books()

    print(books.head(2))

    # tg_publisher.publish(books)
