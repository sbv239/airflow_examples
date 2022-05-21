from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'step8nazarov',
        # Параметры по умолчанию для тасок
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        # Описание DAG (не тасок, а самого DAG)
        description='step8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['nazarov8'],
) as dag:

    def x_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def x_pull(ti):
        testing_increases = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='x_push'
        )
        print(testing_increases)

    t1 = PythonOperator(
        task_id='x_push',
        python_callable=x_push,
    )
    t2 = PythonOperator(
        task_id='x_pull',
        python_callable=x_pull,
    )

    t1 >> t2