import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

import parse
import publish

sys.path.append("/home/o_golovkina/project/source_code/book-stores-parsing/")


def main():
    default_args = {"owner": "Olga Golovkina", "start_date": datetime.now().date()}

    with DAG(
        "book_stores_proccessing",
        default_args=default_args,
        schedule_interval="@daily",
        dagrun_timeout=timedelta(hours=9),
    ):
        air_parsing = PythonOperator(
            task_id="parsing_books", python_callable=parse.main
        )
        air_publishing = PythonOperator(
            task_id="publishing_books_to_telegram", python_callable=publish.main
        )

    air_parsing >> air_publishing


if __name__ == "__main__":
    main()
