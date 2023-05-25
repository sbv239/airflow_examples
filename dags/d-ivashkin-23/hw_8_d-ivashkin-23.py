from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

"""
Сделайте новый DAG, содержащий два Python оператора. Первый PythonOperator должен класть в XCom значение "xcom test" 
по ключу "sample_xcom_key".

Второй PythonOperator должен доставать это значение и печатать его. Настройте правильно последовательность операторов.

Посмотрите внимательно, какие аргументы мы принимали в функции, когда работали с XCom.

NB! Давайте своим DAGs уникальные названия. Лучше именовать их в формате hw_{логин}_1, hw_{логин}_2
"""


def sample_push(ti):
    ti.xcom_push(
        key='sample_xcom_key',
        value='xcom test'
    )


def sample_pull(ti):
    value = ti.xcom_pull(
        key='sample_xcom_key',
        task_ids='xcom_push'
    )
    print(value)


with DAG(
    'hw_d-ivashkin-23_8',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Homework 8-th step DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 21),
    catchup=False,
    tags=['homework', 'di']
) as dag:

    task_push = PythonOperator(
        task_id='xcom_push',
        python_callable=sample_push
    )

    task_pull = PythonOperator(
        task_id='xcom_pull',
        python_callable=sample_pull
    )

task_push >> task_pull
