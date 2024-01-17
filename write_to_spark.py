from hydra import compose, initialize
from pyspark.sql import SparkSession

from books_stores_parsing.parsing import parse


def run_spark():
    spark_session = (
        SparkSession.builder.master("yarn").appName("book_stores_parsing").getOrCreate()
    )

    path_cfg = compose(config_name="path_config")

    books = (
        spark_session.read.option("header", "True")
        .option("delimiter", ";")
        .csv(path_cfg["hadoop_books"], sep=";")
    )

    books.show(2)

    # books.repartition("date", "store_id").write.mode("overwrite").partitionBy(
    #     "date", "store_id"
    # ).format("parquet").option("header", "true").save(path_cfg["spark_books"])


def main():
    initialize(
        version_base=None, config_path="configs", job_name="books_stores_parsing"
    )

    parse()
    run_spark()


if __name__ == "__main__":
    main()
