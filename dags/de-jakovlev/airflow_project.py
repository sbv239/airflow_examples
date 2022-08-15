from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent
from airflow.models import Variable
from airflow.hooks.base_hook import BaseHook

from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_bd_data():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:  # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT "user".id, COUNT(feed_action.action) as likes FROM "user"
                INNER JOIN feed_action
                ON "user".id = feed_action.user_id
                WHERE action = 'like'
                GROUP BY "user".id
                ORDER BY likes DESC
                LIMIT 1
                """
            )
            results = cursor.fetchone()
            return {'user_id': results[0], 'count': results[1]}


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
    'hw_11_de-jakovlev',
    start_date=datetime(2021, 1, 1),
    max_active_runs=2,
    schedule_interval=timedelta(minutes=30),
    default_args=default_args,
    catchup=False
) as dag:

    p1 = PythonOperator(
        task_id='database',
        python_callable=get_bd_data,
    )









