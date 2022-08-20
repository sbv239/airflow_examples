from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'rom_hw_3',
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
    description='A simple tutorial DAG',
    # Как часто запускать DAG
    schedule_interval=timedelta(days=1),
    # С какой даты начать запускать DAG
    # Каждый DAG "видит" свою "дату запуска"
    # это когда он предположительно должен был
    # запуститься. Не всегда совпадает с датой на вашем компьютере
    start_date=datetime(2022, 8, 19),
    # Запустить за старые даты относительно сегодня
    # https://airflow.apache.org/docs/apache-airflow/stable/dag-run.html
    catchup=False,
    # теги, способ помечать даги
    tags=['rom_hw_3'],
) as dag:

    def task_number(task_number, **kwargs):
        print(f'task number is: {task_number}')


    for i in range(30):
        if i <= 10:
            t1 = BashOperator(
                task_id='bash_operator_' + str(i),
                bash_command=f"echo {i}"
            )
        if i > 10:
            t2 = PythonOperator(
                task_id='python_operators_' + str(i),
                python_callable=task_number,
                op_kwargs={'task_number': i}
            )