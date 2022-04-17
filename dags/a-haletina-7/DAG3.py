"""
Test documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG


from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'hw_2_a-haletina-7',

    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2_a-haletina-7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_2_a-haletina-7'],
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command=f'echo {i}',
        )

    def print_number(task_number):
        print(f'task number is: {task_number}')
        return None

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=print_number,
            op_kwargs={'task_number': i}
        )
    t1 >> t2 

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    You can document your task using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in *the UI's* Task **Instance Details page**.
    """
    )

