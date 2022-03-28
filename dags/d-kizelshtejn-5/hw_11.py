from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def user_id_max_like():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action) AS count_like
                FROM "feed_action"
                WHERE action = "like"
                GROUP BY user_id
                ORDER BY count_like DESC
                LIMIT 1
                """
            )
            return cursor.fetchall()


with DAG(
        'hw_11_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_11',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_11']
) as dag:

    t1 = PythonOperator(
        task_id='user_id_max_like',
        python_callable=user_id_max_like,
    )

    t1.doc_md = dedent(
        """
        ## Создаем __DAG__ _**типа `PythonOperator`**_
        подключаемся к таблице _"feed_action"_ и ищем пользователя с наибольшим количеством лайков
        """
    )

    t1
