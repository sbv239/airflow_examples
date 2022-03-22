from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def get_connection():
    from airflow.providers.postgres.operators.postgres import PostgresHook

    postgres = PostgresHook(postgres_conn_id="startml_feed")
    with postgres.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                    select user_id,count(*) from feed_action
                    where action = 'like'
                    group by user_id
                    order by count(*) desc
                    limit 1
                """
            )
            return cursor.fetchall()


with DAG(
        'tarasova_task10',
        # Default settings applied to all tasks
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='DAG for task 10 Tarasova E',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 19),
        catchup=False,
        tags=['task10']

) as dag:
    t1 = PythonOperator(
        task_id='get_user',
        python_callable=get_connection,
    )
    t1
