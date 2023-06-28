from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_11_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_11_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:
    def max_likes():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
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

    t1 = PythonOperator(
        task_id='max_likes',
        python_callable=max_likes,
    )

    t1
