import json
import requests
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

        
with DAG(
    'task_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='An attempt to create DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['attempt'],
) as dag:
    
    def max_feedback():
        from psycopg2.extras import RealDictCursor # это надо, чтобы результаты вернулись в формате словаря
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT f.user_id, COUNT(f.user_id)
                    FROM feed_action f
                    WHERE f.action = "like"
                    GROUP BY f.user_id
                    ORDER BY COUNT(f.user_id) DESC
                    LIMIT 1
                    """
                )
                results = cursor.fetchone()
        return results
    
    
    task = PythonOperator(
        task_id = "not_simple_task",
        python_callable=max_feedback
    )
    
""" можно добавить еще эту таску:
t2 = PythonOperator(
task_id = "print_operator",
doc_md="Распечатать значение",
python_callable=lambda ti: print(ti.xcom_pull(task_ids="not_simple_task", key="return_value")
)

task >> t2
"""