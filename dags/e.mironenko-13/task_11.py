from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from datetime import timedelta, datetime
from airflow.providers.postgres.hooks.postgres import PostgresHook

postgres = PostgresHook(postgres_conn_id="startml_feed")

def conn_data():
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
        """
        select user_id, count(*)
        from feed_action
        where action='like'
        group by user_id
        order by count(*) desc
        limit 1
        """
        )
            return cursor.fetchone()

with DAG(
    'hw_e.mironenko-13_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
            },
    start_date=datetime(2023, 6, 26),
    schedule_interval=timedelta(days=1),
    catchup=False,
    tags = ['e.mironenko-13']
) as dag:

    t1 = PythonOperator(
        task_id='conn_data',
        python_callable=conn_data,
    )

t1