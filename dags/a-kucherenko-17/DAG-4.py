from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.decorators import task
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'kucherenko-hw4',
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
        # Описание DAG (не тасок, а самого DAG)
        description='a-kucherenko-17-dag-4',
        # Как часто запускать DAG
        schedule_interval=timedelta(days=1),
        # С какой даты начать запускать DAG
        # Каждый DAG "видит" свою "дату запуска"
        # это когда он предположительно должен был
        # запуститься. Не всегда совпадает с датой на вашем компьютере
        start_date=datetime(2023, 2, 21),
        # Запустить за старые даты относительно сегодня
        # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
        catchup=False,
        # теги, способ помечать даги
        tags=['kucherenko-hw4'],

) as dag:
    def print_echo(random_base):
        return f"echo {random_base}"


    for i in range(10):
        task1 = BashOperator(
            task_id='DAG3_' + str(i),
            bash_command=f"echo {i}",
        )

    def print_number(task_number):
        return f"task number is: {task_number}"

    for j in range(10, 30):
        task2 = PythonOperator(
            task_id='DAG3_' + str(j),
            python_callable=print_number,
            op_kwargs={'task_number': j},
        )

    task1.doc_md = dedent(
        """
        `code`
        #### Purpose
        Do some __IMPORTANT__ _things_
        """
    )

    task2.doc_md = dedent(
        """
        `code` 
        
        #### Purpose
        Do some __IMPORTANT__ _things_
        """
    )

    task1 >> task2
