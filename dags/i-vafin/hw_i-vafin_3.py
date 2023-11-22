"""
Task-3: динамическое создание задач
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent


def print_task_number(task_number):
    """
    Выводит номер задачи
    """

    print(f'task number is: {task_number}')

    return 'OK'


with DAG(
    'hw_i-vafin_3_dag',
    default_args={},
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 22),
    tags=['hw_i-vafin'],
) as dag:
    dag.doc_md = __doc__

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'bash_autogenerated_task_{i}',
            bash_command=f'echo {i}'
        )

    for i in range(20):
        python_task = PythonOperator(
            task_id=f'python_autogenerated_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
        )

    bash_task.doc_md = dedent(
        """\
    # Документация для задачи

    ## Название
    bash_autogenerated_task_{task_number}, где `{task_number}` - номер задачи, формируемый в **Airflow**

    ## Общее описание
    Задача выполняет _системную_ команду `echo`

    ## Особенности
    Airflow динамические создает 10 аналогичных задач, в каждую из которых, передает номер задачи (нумерация от 0 до 9)
    """
    )

    python_task.doc_md = dedent(
        """\
    # Документация для задачи

    ## Название
    python_autogenerated_task_{task_number}, где `{task_number}` - номер задачи, формируемый в **Airflow**

    ## Общее описание
    Задача печатает в лог _фразу_:  `task number is: {task_number}`

    ## Особенности
    Airflow динамические создает 20 аналогичных задач, в каждую из которых, передает номер задачи (нумерация от 0 до 19)
    """
    )

    bash_task >> python_task
