from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook


def get_ids():
    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            b = cursor.execute('''
                SELECT user_id, count(*)
                from feed_action
                where action = 'like'
                group by user_id
                order by count(*) DESC
                limit 1
                ''')
            return b


with DAG(
        'hw_9_m-zaliskij',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        start_date=datetime(2022, 1, 1),
        catchup=False

) as dag:
    t1 = PythonOperator(
        task_id='get_ids',
        python_callable=get_ids
    )
