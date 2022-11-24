from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2',
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
    description='a-chernenko-30_DAG_hw2',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 11, 23),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['hw_2'],
) as dag:

    # t1
    
    t1 = BashOperator(
        task_id='hw_2',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )


    
    def print_date(ds, **kwargs):
        # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
        print(ds)
        return 'print_ds'

    t2 = PythonOperator(
        task_id='print_python',  # нужен task_id, как и всем операторам
        python_callable=print_date,  # свойственен только для PythonOperator - передаем саму функцию
        )
    # А вот так в Airflow указывается последовательность задач
    t1 >> t2
    # будет выглядеть вот так
    #   t1  -> t2