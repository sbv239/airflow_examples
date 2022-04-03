from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'HW_1_a-betin-5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_retry': False,
        'email_on_failure': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='A first DAG',
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 2, 10),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['first'],
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_dir',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )


    def print_date(ds):
        """Пример PythonOperator"""
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)

    t2 = PythonOperator(
        task_id='date_start',
        python_callable=print_date
    )

    t1 >> t2