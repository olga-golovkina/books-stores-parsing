import os
from pathlib import Path
from subprocess import PIPE, Popen

import pandas as pd

# from hydra import compose, initialize


def main():
    # initialize(
    #     version_base=None, config_path="configs", job_name="books_stores_parsing"
    # )
    #
    # category_ids = compose(config_name="book_categories")
    # store_ids = compose(config_name="store_ids")

    # rest_req = RestRequester()
    #
    # litres_pars = LitresBookParser(rest_req, category_ids, store_ids["litres"])
    #
    # book24_pars = Book24BookParser(rest_req, category_ids, store_ids["book24"])
    #
    # threads = [
    #     ThreadWithValue(target=litres_pars.parse_books_all_types),
    #     ThreadWithValue(target=book24_pars.parse_books_all_types),
    # ]
    #
    # for thread in threads:
    #     thread.start()
    #
    # for thread in threads:
    #     thread.join()
    #
    # res = pd.concat(
    #     [thread.get_result() for thread in threads if thread.get_result() is not None]
    # ).reset_index(drop=True)

    res = pd.read_csv("./output/books.txt", sep=";")

    PATH = Path(
        "/opt/hadoop/airflow/dags/o_golovkina/book_stores_parser/data/books.txt"
    )
    HADOOP_PATH = Path("/user/o_golovkina/book_stores_parsing/books.txt")

    with open(os.open(PATH.absolute(), os.O_RDWR, mode=777), "w") as file:
        text_data = res.to_string(header=False, index=False, decimal=";")
        file.write(text_data)

    create_dir = Popen(
        ["hdfs", "dfs", "-mkdir", "-p", HADOOP_PATH.parents[0].absolute()]
    )
    create_dir.communicate()

    put = Popen(
        ["hdfs", "dfs", "-put", "-f", PATH, HADOOP_PATH], stdin=PIPE, bufsize=-1
    )
    put.communicate()


if __name__ == "__main__":
    main()
