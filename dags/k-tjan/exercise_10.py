#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

# import requests
# import json

def test_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook
    
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
      with conn.cursor() as cursor:
        cursor.execute("""
        SELECT user_id, COUNT(action) count
        FROM feed_action
        WHERE action = 'like'
        GROUP BY user_id, action
        ORDER BY count DESC
        LIMIT 1
        """)
        results = cursor.fetchone()
        print(results)
    # return {'user_id': <идентификатор>, 'count': <количество лайков>}


with DAG(
    'k-tjan_exercise_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='Exercise_10 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 12, 17),
    catchup=False,
    tags=['exercise_10'],
) as dag:

    t1 = PythonOperator(
        task_id='task_push',
        python_callable=test_connection,
        )

    t1
