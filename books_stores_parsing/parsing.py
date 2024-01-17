from pathlib import Path
from subprocess import PIPE, Popen

import pandas as pd
from hydra import compose, initialize
from hydra.core.global_hydra import GlobalHydra

from books_stores_parsing.book24.parser import Book24BookParser
from books_stores_parsing.litres.parser import LitresBookParser
from books_stores_parsing.livelib.parser import LivelibBookParser
from books_stores_parsing.multithreading.thread_with_value import ThreadWithValue
from books_stores_parsing.requesting.rest_requester import RestRequester


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

    category_ids = compose(config_name="book_categories")
    store_ids = compose(config_name="store_ids")

    rest_req = RestRequester()

    litres_pars = LitresBookParser(rest_req, category_ids, store_ids["litres"])
    livelib_pars = LivelibBookParser(rest_req, category_ids, store_ids["livelib"])
    book24_pars = Book24BookParser(rest_req, category_ids, store_ids["book24"])

    threads = [
        ThreadWithValue(target=litres_pars.parse_books_all_types),
        ThreadWithValue(target=livelib_pars.parse_books_all_types),
        ThreadWithValue(target=book24_pars.parse_books_all_types),
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    res = pd.concat(
        [
            thread.get_result()
            for thread in threads
            if thread.get_result() is not None and len(thread.get_result().index) > 0
        ]
    ).reset_index(drop=True)

    books_path = Path(path_cfg["parsed_books"])
    hadoop_books_path = Path(path_cfg["hadoop_books"])

    res.to_csv(books_path, index=False, sep=";")

    create_hadoop_directory(hadoop_books_path.parents[0])
    create_hadoop_directory(Path(path_cfg["spark_books"]))

    put = Popen(
        ["hdfs", "dfs", "-put", "-f", books_path, hadoop_books_path],
        stdin=PIPE,
        bufsize=-1,
    )
    put.communicate()


if __name__ == "__main__":
    parse()
