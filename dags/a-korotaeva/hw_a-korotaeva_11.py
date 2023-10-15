from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id='startml_feed')

def conn_data():
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute("""SELECT user_id, COUNT(post_id) FROM feed_action WHERE action = 'like' GROUP BY user_id ORDER BY COUNT(post_id) DESC LIMIT 1;""")
            return cursor.fetchone()


with DAG(
    'hw_11_1_a-korotaeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG', schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 10, 13),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id='get_likes', python_callable=conn_data)

    t1