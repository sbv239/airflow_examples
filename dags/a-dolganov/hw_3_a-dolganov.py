from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-dolganov_3',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='a-dolganov second DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['a-dolganov'],
) as dag:

    def print_num(task_number, ts, run_id):
        print(ts)
        print(run_id)
        return f"task number is: {task_number}"

    for i in range(30):
        if i < 10:
            task = BashOperator(
                    task_id = 'print_num_' + str(i),
                    bash_command = "echo $NUMBER",
                    env = {"NUMBER": str(i)},
            )
        else:
            task = PythonOperator(
                    task_id ='print_num_' + str(i),
                    python_callable = print_num,
                    op_kwargs = {'task_number': i},
            )
    task.doc_md = dedent(
        """\
#### Task Documentation
#### `code`
#### **полужирный**
#### _курсив_
"""
    )

    task