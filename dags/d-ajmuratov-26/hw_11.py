from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.postgres.hooks.postgres import PostgresHook

with DAG(
    'hw_d-ajmuratov-26_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
        },
    schedule_interval=timedelta(days=1),
    start_date=datetime.now(),
    catchup=False,
    tags=['HW 11']
) as dag:

    def get_most_like():
        postgres = PostgresHook(postgres_conn_id="startml_feed")
        with postgres.get_conn() as conn:   # вернет тот же connection, что вернул бы psycopg2.connect(...)
            with conn.cursor() as cursor:
                # your code
                cursor.execute("""
                    select user_id, count(*) as count
                    from feed_action
                    where action = 'like'
                    group by user_id
                    order by count desc
                    limit 1
                """)
                result = cursor.fetchone()
        return result
    
    t = PythonOperator(
        task_id='get_most_like',
        python_callable=get_most_like
    )
    t

