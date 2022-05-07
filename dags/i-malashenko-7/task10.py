"""
Test documentation
"""
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent

import requests
import json

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
with DAG(
    'hw_10_i-malashenko-7',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 20),
    catchup=False,
    tags=['example'],
) as dag:

    def get_data():
        from airflow.providers.postgres.operators.postgres import PostgresHook

        postgres = PostgresHook(postgres_conn_id='startml_feed')

        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                """
                    SELECT user_id, COUNT(action)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(action) DESC
                    LIMIT 1
                """
                )
                return cursor.fetchall()

    t1 = PythonOperator(
        task_id='get_data',
        python_callable=get_data
    )