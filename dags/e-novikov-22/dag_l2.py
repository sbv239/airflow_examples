"""
2 step. Execute pwd in bash and print ds
"""
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hw_2_e-novikov-22', # имя dag

    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['e-novikov-22'],
) as dag:

    t1 = BashOperator(
        task_id='execute_pwd',
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )

    def print_logical_date(ds):
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_logical_date,
    )

    t1 >> t2
