"""
hw_10_m_zharehina_5
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

from airflow.providers.postgres.operators.postgres import PostgresHook


def postgres():
    postgres = PostgresHook(postgres_conn_id='startml_feed')
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute('''
                    SELECT user_id, count(post_id)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count(post_id) DESC''')
            return print(cursor.fetchone())

        
with DAG(
    'hw_10_m_zharehina_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    description='hw_10_m_zharehina_5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 26),
    catchup=False,
    tags=['hw_10_m_zharehina_5'],
    ) as dag:   
    
    task1 = PythonOperator(task_id='hw_10_m_zharehina_5', python_callable=postgres)