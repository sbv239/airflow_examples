from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

from psycopg2.extras import RealDictCursor

ID = "hw_11_n-chistjakov_"
conn_id = "startml_feed"
postgres = PostgresHook(postgres_conn_id=conn_id)

def get_toplike_user():
    with postgres.get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor  ) as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(action)
                FROM feed_action f
                WHERE f.action = 'like'
                GROUP BY user_id    
                ORDER BY COUNT(action) DESC
                """
            )
            return cursor.fetchone()


def print_info(ts,  run_id, **kwargs):
    print(ts)
    print(run_id)
    print(f'task number is: {kwargs["task_number"]}'    )

with DAG(
    'hw_11_n-chistjakov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description="Second task",
    start_date=datetime(2023, 6, 30),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags=["task_11"],
) as dag:

    pyth = PythonOperator(
        task_id=ID + "1",
        python_callable=get_toplike_user
    )

    pyth