import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from books_stores_parsing import parsing as parser
from books_stores_parsing import publishing as publisher

sys.path.append("/home/o_golovkina/book_stores_parsing/")


def main():
    default_args = {"owner": "Olga Golovkina", "start_date": datetime(2024, 1, 15)}

    with DAG(
        "book_stores_proccessing",
        default_args=default_args,
        schedule_interval="@daily",
        dagrun_timeout=timedelta(hours=9),
    ):
        air_parsing = PythonOperator(
            task_id="parsing_books", python_callable=parser.main
        )
        air_posting = PythonOperator(
            task_id="publishing_books_to_telegram", python_callable=publisher.main
        )

    air_parsing >> air_posting


if __name__ == "__main__":
    main()
