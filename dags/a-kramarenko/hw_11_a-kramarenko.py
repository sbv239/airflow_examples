from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from psycopg2.extras import RealDictCursor

with DAG(
    'hw_a-kramarenko_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 10 DAG',
    start_date=datetime(2023, 10, 26),
) as dag:

    def get_user():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT user_id, COUNT(user_id)
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY COUNT(user_id) desc
                    limit 1
                    """
                )
                result = cursor.fetchone()
        return result

    task1 = PythonOperator(
            task_id = 'get_user',
            python_callable= get_user
        )
    
    task1 
