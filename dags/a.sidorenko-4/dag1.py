from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator, PythonOperator
with DAG(
    'tutorial',
    # Параметры по умолчанию для тасок
    default_args={
        # Если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале?
        'email_on_failure': False,
        # Писать ли при автоматическом перезапуске по провалу
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # # Описание DAG (не тасок, а самого DAG)
    # description='A simple tutorial DAG',
    # # Как часто запускать DAG
    # schedule_interval=timedelta(days=1),
    # # С какой даты начать запускать DAG
    # # Каждый DAG "видит" свою "дату запуска"
    # # это когда он предположительно должен был
    # # запуститься. Не всегда совпадает с датой на вашем компьютере
    # start_date=datetime(2022, 1, 1),
    # # Запустить за старые даты относительно сегодня
    # # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    # catchup=False,
    # # теги, способ помечать даги
    # tags=['example'],
) as dag:



    def print_date(ds, **kwargs):
        print(ds)


    run_bash = BashOperator(
        task_id='current working directory',
        depends_on_past=False,  # переопределили настройку из DAG
        bash_command='pwd',
        retries=3,  # тоже переопределили retries (было 1)
    )

    run_python = PythonOperator(
        task_id='print_date',  # id, будет отображаться в интерфейсе
        python_callable=print_date
    )

    run_bash >> run_python
