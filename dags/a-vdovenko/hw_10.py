from datetime import timedelta, datetime
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.operators.python import PythonOperator

def postgres_like():
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


with DAG(
    'hw_10_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Lesson 11 home work 10",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    task = PythonOperator(
        task_id = 'max_like_per_user',
        python_callable = postgres_like
    )
    task