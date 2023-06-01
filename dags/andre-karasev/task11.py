from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook


def sql_request():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
              """
              SELECT user_id, COUNT(*)
              FROM feed_action
              WHERE action = 'like'
              GROUP BY user_id
              ORDER BY COUNT(*) DESC 
              """
            )
            return cursor.fetchone()

default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    }

with DAG('andre-karasev_hw_11',
         default_args=default_args,
         description='hw_11_',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2022, 9, 9),
         catchup=False,
         tags=['andre-karasev_hw_11']) as dag:
    t1 = PythonOperator(
        task_id="sql_request",
        python_callable=sql_request
    )
    t1