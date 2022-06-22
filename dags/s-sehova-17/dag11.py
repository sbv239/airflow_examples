from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

def get_conn():
    from airflow.hooks.base import BaseHook
    import psycopg2

    creds = BaseHook.get_connection(postgres_conn_id="startml_feed")
    with psycopg2.connect(
        f"postgresql://{creds.login}:{creds.password}"
        f"@{creds.host}:{creds.port}/{creds.schema}"
    ) as conn:
    with conn.cursor() as cursor:
        cursor.execute(
            """"
                select user_id, count(*)
                from feed_action
                order by count(*) desc
                limit 1
            """"
        )
        return cursor.fetchall()


with DAG(
    's-sehova-17-11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),

    catchup=False,
    tags=['task11'],
) as dag:
    t1 = PythonOperator(
        task_id = connection,
        python_callable = get_conn,
        )
t1
        
    
    
        