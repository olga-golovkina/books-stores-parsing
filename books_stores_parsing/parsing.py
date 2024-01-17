import os
from pathlib import Path
from subprocess import PIPE, Popen

import pandas as pd
from hydra import compose, initialize
from hydra.core.global_hydra import GlobalHydra


def create_hadoop_directory(path: Path):
    if path.is_file():
        path = path.parents[0].absolute()

    create_dir = Popen(
        ["hdfs", "dfs", "-mkdir", "-p", path],
        stdin=PIPE,
        bufsize=-1,
    )
    create_dir.communicate()


def parse():

    if not GlobalHydra.instance().is_initialized():
        initialize(
            version_base=None, config_path="../configs", job_name="books_stores_parsing"
        )

    path_cfg = compose(config_name="path_config")

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

    res = pd.read_csv("../output/books.txt", sep=";")

    books_path = Path(path_cfg["parsed_books"])
    raw_books_hadoop_path = Path(path_cfg["hadoop_books"])

    with open(os.open(books_path.absolute(), os.O_RDWR, mode=777), "w") as file:
        text_data = res.to_string(header=False, index=False, decimal=";")
        file.write(text_data)

    create_hadoop_directory(raw_books_hadoop_path)
    create_hadoop_directory(Path(path_cfg["spark_books"]))

    put = Popen(
        ["hdfs", "dfs", "-put", "-f", books_path, raw_books_hadoop_path],
        stdin=PIPE,
        bufsize=-1,
    )
    put.communicate()


if __name__ == "__main__":
    parse()
