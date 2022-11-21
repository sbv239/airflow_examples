from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

import pandas as pd

from airflow.providers.postgres.operators.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="postgresql://robot-startml-ro:pheiph0hahj1Vaif@"
    "postgres.lab.karpov.courses:6432/startml")

with postgres.get_conn() as conn: 
    with conn.cursor() as cursor:
        likers = cursor.execute("""
        SELECT post_id, COUNT(action='like') AS likes
        FROM "feed_action"
        WHERE action='like'
        GROUP BY post_id
        ORDER BY likes DESC
        """)
        liker = likers.iloc[0]

    with DAG(
        'j-jancharskaja_10',
        default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
        description = 'My eigth DAG',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 11, 11),
        catchup = False,
        tags = ['eigth']
    ) as dag:

        def push_test():
            return 'Airflow tracks everything'
    
        t1 = PythonOperator(
            task_id = 'push_test',
            python_callable = push_test,
            )
