import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import requests
import json

conn_id="startml_feed"

def return_string():
    return 'Airflow tracks everything'

def user_max_like():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id=conn_id)
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
            """
           SELECT user_id, COUNT(post_id)  
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(post_id) DESC
          """
        )
        print(cursor.fetchone())

with DAG(
        # Название таск-дага
        'hw_10_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 10',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_10']

) as dag:

    t1 = PythonOperator(
        task_id='user_max_like',
        python_callable=user_max_like,
        #op_kwargs={'task_number': i},
    )




    t1