"""
Task-7: динамическое создание задач и работа с аргументами
"""

from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime
from textwrap import dedent

from default_args import default_args


def print_task_number(ts, run_id, task_number):
    """
    Выводит номер задачи и др аргументы
    """

    print(f'task number is: {task_number}')
    print(f'ts is: {ts}')
    print(f'run_id is {run_id}')

    return 'OK'


with DAG(
    'hw_i-vafin_7_dag',
    default_args=default_args,
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 22),
    tags=['hw_i-vafin'],
) as dag:
    dag.doc_md = __doc__

    for i in range(20):
        python_task = PythonOperator(
            task_id=f'python_autogenerated_task_{i}',
            python_callable=print_task_number,
            op_kwargs={'task_number': i}
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

    python_task
