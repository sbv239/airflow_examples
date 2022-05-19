from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'step2_v-nazarov',
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
    description='step2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['v-nazarov'],
) as dag:


    for i in range(10):
        t1 = BashOperator(
            task_id='loop_bash_' + str(i),
            bash_command=f"echo {i}",
        )

    def task_number(task_number):
        print(f"task number is: {task_number}")
    for i in range(20):
        t2 = PythonOperator(
            task_id='my_loop_py_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=task_number,
            op_kwargs={'task_number': i},
        )

    t1 >> t2