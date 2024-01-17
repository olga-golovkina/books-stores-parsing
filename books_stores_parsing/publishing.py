# from pathlib import Path
from hydra import compose, initialize
from hydra.core.global_hydra import GlobalHydra
from pandas import DataFrame
from pyspark.sql import SparkSession

# from books_stores_parsing.telegram.bot_publisher import TelegramBotPublisher
# from books_stores_parsing.telegram.post_creator import TelegramPostCreator
# from books_stores_parsing.telegraph.article_creator import TelegraphHtmlArticleCreator
# from books_stores_parsing.telegraph.article_publisher import TelegraphArticlePublisher
from pyspark.sql.types import DateType, FloatType, IntegerType, StringType, StructType


def create_schema():
    return (
        StructType()
        .add("datetime", DateType())
        .add("store_id", IntegerType())
        .add("url", StringType())
        .add("title", StringType())
        .add("img_url", StringType())
        .add("isbn", StringType())
        .add("description", StringType())
        .add("rating", FloatType())
        .add("price", IntegerType())
        .add("category_id", IntegerType())
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
