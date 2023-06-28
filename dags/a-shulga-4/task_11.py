from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

def psycopg():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
        with conn.cursor() as cursor:
            cursor.execute(
                    """
                    SELECT user_id, COUNT(action) AS count
                    FROM feed_action
                    WHERE action = 'like'
                    GROUP BY user_id
                    ORDER BY count DESC
                    LIMIT 1
                    """
                    )
            return cursor.fetchone()


with DAG(
    'hw_a-shulga-4_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 6, 27),
    catchup=False
    ) as dag:

        
        t1 = PythonOperator(
                task_id='pg_connection_test',
                python_callable=psycopg
        )

        t1