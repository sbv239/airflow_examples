from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'step2 v-nazarov',
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
    def my_print_function(ds):
        print(f'task number is: {ds}')
        return 0

    for i in range(10):
        taskx = BashOperator(
            task_id='loop_bash_' + str(i),
            depends_on_past=False,
            bash_command=f"echo {i}",
        )
        # tloop0 >> taskx

    for i in range(20):
        taskp = PythonOperator(
            task_id='my_loop_py_'+ str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=my_print_function,
            op_kwargs={'ds': str(i)},
        )