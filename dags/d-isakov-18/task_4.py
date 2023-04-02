"""
Task 4
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_4_d-isakov-18',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description=__doc__,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id='command_echo_' + str(i),
            bash_command=f"echo {i}",
        )
    t1.doc_md = dedent(
        """\
    # Task Documentation
    You can *document* your **task** using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    """
    )


    def print_task_number(task_number):
        print(f'task number is: {task_number}')

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )

    t2.doc_md = dedent(
        """\
    # Task Documentation
    You can *document* your **task** using the attributes `doc_md` (markdown),
    `doc` (plain text), `doc_rst`, `doc_json`, `doc_yaml` which gets
    rendered in the UI's Task Instance Details page.
    """
    )

    t1 >> t2
