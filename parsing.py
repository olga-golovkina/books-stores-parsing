import pandas as pd
from hydra import compose, initialize

from books_stores_parsing.book24.parser import Book24BookParser
from books_stores_parsing.litres.parser import LitresBookParser
from books_stores_parsing.multithreading.thread_with_value import ThreadWithValue
from books_stores_parsing.requesting.rest_requester import RestRequester


def main():
    initialize(
        version_base=None, config_path="configs", job_name="books_stores_parsing"
    )

    category_ids = compose(config_name="book_categories")
    store_ids = compose(config_name="store_ids")

    selenium_req = RestRequester()

    litres_pars = LitresBookParser(selenium_req, category_ids, store_ids["litres"])

    book24_pars = Book24BookParser(selenium_req, category_ids, store_ids["book24"])

    threads = [
        ThreadWithValue(target=litres_pars.parse_books_all_types),
        ThreadWithValue(target=book24_pars.parse_books_all_types),
    ]

    for thread in threads:
        thread.start()

    for thread in threads:
        thread.join()

    res = pd.concat(
        [thread.get_result() for thread in threads if thread.get_result() is not None]
    ).reset_index(drop=True)

    res.to_csv("./output/books.txt", sep=";", index=False)  # TODO: положить в hadoop


if __name__ == "__main__":
    main()
