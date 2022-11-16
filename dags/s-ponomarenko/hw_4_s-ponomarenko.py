# s-ponomarenko мой логин

from datetime import datetime, timedelta
from textwrap import dedent
#Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

#Операторы - это кирпичики DAG, оня являются звеньями в графе.
#Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_3_s-ponomarenko',
    #Параметры по умолчанию для тасков
    default_args={
        # если прошлые запуски упали, надо ли ждать их успеха
        'depends_on_past': False,
        # Кому писать при провале
        'email': ['airflow@example.com'],
        # А писать ли вообще при провале
        'email_on_failure': False,
        # писать ли при автоматическом перезапуске при провале
        'email_on_retry': False,
        # Сколько раз пытаться запустить, далее помечать как failed
        'retries': 1,
        # Сколько ждать между перезапусками
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    # Описание DAG (не тасков, а самого DAG)
    description = 'DAG for homework - step 3',
    # Как часто запускать DAG (days=1 - каждый день)
    schedule_interval = timedelta(days=1),
    # С какой даты начать запускать DAG.
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно был запуститься.
    # Не всегда совпадает с датой на вашем компьютере.
    start_date = datetime(2022,11,15),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # Теги, способы помечать DAG
    tags = ['homework_s-ponomarenko'],
) as dag:

    for i in range(10):
        task_bash = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command = f"echo {i}",
        )
    task_bash.doc_md = dedent(
        """\
        # My BashTask documentation - header line
        ## My BashTask documentation - 2nd header line
        ### My BashTask documentation - 3rd header line
        *bla-bla in italic*
        _bla-bla in italic - alt method_
        **bla-bla in semi-bold**
        __bla-bla in semi-bold - alt method__
        `code highlighting`
        
        """
    )  # dedent - это особенность Airflow, в него нужно оборачивать весь док
    def print_py_task(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        task_python = PythonOperator(
            task_id='python_task_' + str(i),
            python_callable = print_py_task,
            op_kwargs={'task_number': i},
        )

    task_python.doc_md = dedent(
        """\
        # My BashTask documentation - header line
        ## My BashTask documentation - 2nd header line
        ### My BashTask documentation - 3rd header line
        *bla-bla in italic*
        _bla-bla in italic - alt method_
        **bla-bla in semi-bold**
        __bla-bla in semi-bold - alt method__
        `code highlighting`

        """
    )  # dedent - это особенность Airflow, в него нужно оборачивать весь док

    # последовательность тасков
    task_bash >> task_python