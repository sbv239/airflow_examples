"""
--DAG docs will be there--
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def first_python_operator(*args, **kwargs):
    # from airflow.hooks.base import BaseHook
    # import psycopg2
    #
    # creds = BaseHook.get_connection(id
    # соединения)
    # with psycopg2.connect(
    #         f"postgresql://{creds.login}:{creds.password}"
    #         f"@{creds.host}:{creds.port}/{creds.schema}"
    # ) as conn:
    #     with conn.cursor() as cursor:
    #         ...
    #         # your code

    from airflow.providers.postgres.hooks.postgres import PostgresHook
    # from psycopg2.extras import RealDictCursor

    postgres = PostgresHook(postgres_conn_id="startml_feed", cursor="realdictcursor")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute("""
            SELECT user_id, count(action)
            FROM feed_action
            WHERE action='like'
            GROUP BY user_id
            ORDER BY count(action) DESC""")
            return cursor.fetchone()


default_args={
    'depends_on_past': False,
    'owner': 'and-f',
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(dag_id='hw_11_and-f',
         default_args=default_args,
         description='--DAG description here--',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 7, 17),
         catchup=False,
         tags=['--DAG tag here--']) as dag:

    dag.doc_md = __doc__
    t1 = PythonOperator(task_id='t1', python_callable=first_python_operator)
