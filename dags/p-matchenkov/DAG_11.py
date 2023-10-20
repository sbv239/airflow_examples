import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from datetime import timedelta

from airflow.hooks.base import BaseHook
import psycopg2


def get_user():
    creds = BaseHook.get_connection('startml_feed')
    with psycopg2.connect(
      f"postgresql://{creds.login}:{creds.password}"
      f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT("action")
                FROM feed_action
                WHERE "action" = 'like'
                GROUP BY user_id
                ORDER BY COUNT("action") DESC
                LIMIT 1
                """
        )
            result = cursor.one_or_none()
            return result


with DAG(
    'hw_11_p-matchenkov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    start_date=datetime.datetime(2023, 10, 19),
    catchup=False,
    description='tag 11',
    tags=['matchenkov']
) as dag:

    task_1 = PythonOperator(
        task_id='return_user_from_db',
        python_callable=get_user
    )

    task_1