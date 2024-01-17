# from multiprocessing import Process

# import re
from pathlib import Path

from hydra import compose, initialize

# from pyspark import RDD, SparkContext
from pyspark import RDD
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

from parsing import parse

# from pyspark.streaming import StreamingContext


def handle_stream(record: RDD, session: SparkSession, saving_path: Path):
    if not record.count() == 0:
        books = session.createDataFrame(record)
        books = books.selectExpr(
            "_1 as timestamp",
            "_2 as store_id",
            "_3 as url",
            "_4 as title",
            "_5 as img_url",
            "_6 as author",
            "_7 as isbn",
            "_8 as description",
            "_9 as rating",
            "_10 as price",
            "_11 as category_id",
        )

        books.repartition(2).write().mode("overwrite").partitionBy(
            "store_id", "category_id"
        ).format("parquet").option("compression", "snappy").save(saving_path.absolute())


def run_spark():
    # spark_ctx = SparkContext("yarn", "book_stores_parsing")
    spark_session = (
        SparkSession.builder.master("yarn").appName("textFileStream").getOrCreate()
    )

    path_cfg = compose(config_name="path_config")

    schema = (
        StructType()
        .add("timestamp", "float")
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

    print(books)

    books.repartition(2).write().mode("overwrite").partitionBy(
        "store_id", "category_id"
    ).format("parquet").option("compression", "snappy").save(path_cfg["spark_books"])

    print("Done")

    # stream_ctx = StreamingContext(spark_ctx, 1)
    #
    #
    #
    # input_stream = stream_ctx.textFileStream(path_cfg["hadoop_books"]).map(
    #     lambda file: re.split(r"\s+", file)
    # )
    # input_stream.foreachRDD(
    #     lambda rdd: handle_stream(rdd, spark_session, Path(path_cfg["spark_books"]))
    # )
    # stream_ctx.start()
    # stream_ctx.awaitTermination()


def main():
    parse()
    run_spark()
    # pars_proc = Process(target=parse)
    # spark_proc = Process(target=start_spark)
    #
    # pars_proc.start()
    # spark_proc.start()
    #
    # pars_proc.join()
    # spark_proc.join()


if __name__ == "__main__":
    initialize(
        version_base=None, config_path="configs", job_name="books_stores_parsing"
    )
    main()
