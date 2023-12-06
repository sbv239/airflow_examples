from datetime import datetime, timedelta

from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def get_active_user():
    from psycopg2.extras import RealDictCursor
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(user_id)
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(user_id) DESC
                LIMIT 1
                """
            )
            results = cursor.fetchone()
            return results


with DAG(
    'm-mihail-24_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example_11'],
) as dag:
    p11 = PythonOperator(
        task_id='postgres_query',
        python_callable=get_active_user
    )
    p11_print = PythonOperator(
        task_id='print_result',
        python_callable=lambda ti: print(ti.xcom_push(task_ids='postgres_query', key='return_value'))
    )
    p11 >> p11_print