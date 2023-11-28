# Для объявления DAG импортируем класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_e-shajapin_4',
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
    description='DAG`s Shayapin for StartML_Step4',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    start_date=datetime(2023, 11, 19),
    # Запустить за старые даты относительно сегодня
    catchup=False,
    # теги, способ помечать даги
    tags=['e-shajapin_step_4'],
) as dag:

    # Генерируем таски в цикле
    for i in range(10):
        task_bash = BashOperator(
            task_id=f"print_echo_{i}",  # id, будет отображаться в интерфейсе
            bash_command=f"echo {i}",  # какую bash команду выполнить в этом таске
        )

        task_bash.doc_md = dedent(
            """\
        #### **Task Documentation on `task_bash`**
        You can document your task using the attributes `doc_md` (markdown).
        *Команда `bash` выводит значение задачи*
        """
        )


    def number_task(task_number, **kwargs):
        print(f"task number is: {task_number}")


    # Генерируем таски в цикле
    for i in range(20):
        task_python = PythonOperator(
            task_id=f"print_number_task_{i}",
            python_callable=number_task,
            op_kwargs={'task_number': i},
        )

        task_python.doc_md = dedent(
            """\
        #### **Task Documentation on `task_python`**
        You can document your task using the attributes `doc_md` (markdown).
        *Команда `python` выводит номер задачи*
        """
        )

    task_bash >> task_python
