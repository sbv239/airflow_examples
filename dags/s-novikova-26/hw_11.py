"""
HW 11
"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

def get_user():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                 SELECT user_id, COUNT(user_id)
                 FROM feed_action
                 WHERE action = 'like'
                 GROUP BY user_id
                 ORDER BY COUNT(user_id) DESC
                 LIMIT 1 
                 '''
                           )
            result = cursor.fetchone()
            return result

default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
}

with DAG(
    'hw_11_s-novikova-26',
    default_args=default_args,
    description='HW 11',
    start_date=datetime(2023, 11, 30),
    catchup=False,
    tags=['HW 11']
) as dag:
        t1 = PythonOperator(
            task_id='get_user',
            python_callable=get_user
        )