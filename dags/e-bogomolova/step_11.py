from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def get_user_max_like():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT user_id, COUNT(post_id)  
                FROM feed_action
                WHERE action = 'like'
                GROUP BY user_id
                ORDER BY COUNT(post_id) DESC """
            )
            print(cursor.fetchone())


with DAG(
    'e_bogomolova_step_11',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_11'],
) as dag:

    t1 = PythonOperator(
        task_id='user_max_like',
        python_callable=get_user_max_like,
    )
    t1
