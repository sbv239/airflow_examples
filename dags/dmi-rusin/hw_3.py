from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_dmi-rusin_2',
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
        description='Second task',
        # Как часто запускать DAG
        schedule_interval=timedelta(minutes=5),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 6, 1),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['example'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print_' + str(i),
            bash_command=f'echo {i}',
        )

    def print_task(task_number):
        print("task number is:", task_number)


    for i in range(20):
        t2 = PythonOperator(
            task_id='dmi-rusin_3_tasks'+ str(i),
            python_callable=print_task,
            op_kwargs={'task_number': i},
        )
    t1 >> t2

