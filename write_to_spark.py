from hydra import compose, initialize
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from books_stores_parsing.parsing import parse


def run_spark():
    spark_session = (
        SparkSession.builder.master("yarn").appName("book_stores_parsing").getOrCreate()
    )

    path_cfg = compose(config_name="path_config")

    schema = (
        StructType()
        .add("date", "string")
        .add("store_id", "integer")
        .add("url", "string")
        .add("title", "string")
        .add("img_url", "string")
        .add("author", "string")
        .add("isbn", "string")
        .add("description", "string")
        .add("rating", "float")
        .add("price", "integer")
        .add("category_id", "integer")
    )

    books = spark_session.read.csv(path_cfg["hadoop_books"], sep=";", schema=schema)
    books.repartition(2, "date", "store_id").write.mode("overwrite").partitionBy(
        "date", "store_id"
    ).format("parquet").option("header", "true").save(path_cfg["spark_books"])


def main():
    initialize(
        version_base=None, config_path="configs", job_name="books_stores_parsing"
    )

    parse()
    run_spark()


if __name__ == "__main__":
    main()
