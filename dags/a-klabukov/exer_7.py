
from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG

# Операторы - это кирпичики DAG, они являются звеньями в графе
# Будем иногда называть операторы тасками (tasks)
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
with DAG(
    'a-klabukov_hw_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        t3 = BashOperator(
            task_id= f'a-klabukov_2_{i}',
            bash_command="echo $NUMBER",
            env={'NUMBER': str(i)},
        )
    def print_str(ts, run_id, **kwargs):
        task_number = kwargs['task_number']
        print(ts)
        print(run_id)
        print(f"task number is: {task_number}")



    for i in range(20):
        task = PythonOperator(
            task_id='a_klabukov_2_' + str(i),  # в id можно делать все, что разрешают строки в python
            python_callable=print_str,
            op_kwargs={'task_number': i},
        )


