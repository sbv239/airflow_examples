from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    'hw_j-miller_1',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False,# Если прошлые запуски упали, надо ли ждать их успеха
        'email': ['airflow@example.com'],# Кому писать при провале
        'email_on_failure': False,# А писать ли вообще при провале?
        'email_on_retry': False,# Писать ли при автоматическом перезапуске по провалу
        'retries': 1,# Сколько раз пытаться запустить, далее помечать как failed
        'retry_delay': timedelta(minutes=5),  # Сколько ждать между перезапусками# timedelta из пакета datetime
    },
    description='A simple tutorial DAG',# Описание DAG (не тасок, а самого DAG)
    schedule_interval=timedelta(days=1),# Как часто запускать DAG
    start_date=datetime(2023, 5, 30),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['example'],# теги, способ помечать даги
) as dag:

    # t1, t2, t3 - это операторы (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='make_pwd',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )


    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable = print_context,
    )
    t1 >> t2
