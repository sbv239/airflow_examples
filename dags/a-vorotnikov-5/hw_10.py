from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
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
                ORDER BY count(post_id) DESC'''
                           )
            return print(cursor.fetchone())


with DAG('hw_10_vorotnikov', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}, start_date=datetime(2022, 3, 20), catchup=False) as dag:
    t1 = PythonOperator(task_id='max_like_user', python_callable=postgres)
