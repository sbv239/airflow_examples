from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from textwrap import dedent


with DAG(
        's-vazhenin-17-task_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 15),
        catchup=False,
        tags=['example'],
) as dag:
    for i in range(10):
        t31 = BashOperator(
            task_id = f'task_3_bash_{i}',
            bash_command = f'echo {i}'
            )

    def foorloop_print(task_number):
        print(f'task number is: {task_number}')

    for i in range(20):
        t32 = PythonOperator(
            task_id = f'task_3_python_{i}',
            python_callable=foorloop_print,
            op_kwargs = {'task_number': i}
            )
t32.doc_md = dedent(
    f"""\
    ##Python operator docs: 
    #Simple output: _pythoncomand_=print(task_number:{i})
    """
    )

t31 >> t32