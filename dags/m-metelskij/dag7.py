from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'hw_m-metelskij_2',
    # Параметры по умолчанию для тасок
    default_args=default_args,
    # Описание DAG (не тасок, а самого DAG)
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 1, 1),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['example'],
) as dag:

    for i in range(10):
        bash_task = BashOperator(
            task_id=f'm-metelskij-7_bash_{i}',
            depends_on_past=False,
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i+1)}
        )

    def print_ds(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)
    
    for i in range(20):
        python_task = PythonOperator(
            task_id=f'm-metelskij_7_python_{i}',
            python_callable=print_ds,
            op_kwargs={'task_number': i + 11}
        )

    # bash_task.doc_md = dedent(
    #     """
    #     ### Hi bash
    #     for **10 tasks** *(from 1 to 10)*:
    #     `bash_command=f"echo {i+1}`
    #     """
    #     )
    
    # python_task.doc_md = dedent(
    #     """
    #     ### Hi py
    #     for **20 tasks** *(from 11 to 30)*:
    #     `print(f'task number is: {task_number}')`
    #     """
    # )

    bash_task >> python_task