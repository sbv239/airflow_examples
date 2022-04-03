from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresHook

def my_request():
        postgres = PostgresHook(postgres_conn_id='startml_feed')
        with postgres.get_conn() as conn:
                with conn.cursor() as cursor:
                        cursor.execute(
                                """
                                SELECT user_id, COUNT(action)
                                FROM feed_action
                                WHERE action = 'like'
                                GROUP BY user_id
                                ORDER BY COUNT(action) DESC
                                LIMIT 1
                                """
                        )



with DAG(
    'HW_10_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A 8th DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 4, 2),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['task_8'],
) as dag:
        t1 = PythonOperator(
                task_id = "top_user",
                python_callable = my_request
        )

        t1
