from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG # Для объявления DAG нужно импортировать класс из airflow


from airflow.operators.bash import BashOperator
with DAG(
    'tutorial',
    # Параметры по умолчанию для тасок
    default_args={
        'depends_on_past': False, # Если прошлые запуски упали, надо ли ждать их успеха
        'email': ['airflow@example.com'], # Кому писать при провале
        'email_on_failure': False, # А писать ли вообще при провале?
        'email_on_retry': False, # Писать ли при автоматическом перезапуске по провалу
        'retries': 1, # Сколько раз пытаться запустить, далее помечать как failed
        'retry_delay': timedelta(minutes=5), # Сколько ждать между перезапусками # timedelta из пакета datetime
    },
    # Описание DAG (не тасок, а самого DAG)
    description='The first DAG',
    schedule_interval=timedelta(days=1), # Как часто запускать DAG
    start_date=datetime(2022, 1, 1), # С какой даты начать запускать DAG
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    tags=['example'],  # теги, способ помечать даги
) as dag:

    # t1 - это оператор  (они формируют таски, а таски формируют даг)
    t1 = BashOperator(
        task_id='print_date',  # id, будет отображаться в интерфейсе
        bash_command='pwd',  # какую bash команду выполнить в этом таске
    )


def print_context(ds, **kwargs):
"""Пример PythonOperator"""
    print(kwargs) # Через синтаксис **kwargs можно получить словарь с настройками Airflow. Значения оттуда могут пригодиться.
    print(ds) # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    return 'Whatever you return gets printed in the logs'


run_this = PythonOperator(
    task_id='print_the_context',  # нужен task_id, как и всем операторам
    python_callable=print_context,  # свойственен только для PythonOperator - передаем саму функцию
)