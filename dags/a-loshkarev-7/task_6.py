from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_i(task_number):
    print(f'task number is: {task_number}')
    return f'printed {task_number}'


with DAG(
    'aloshkarev_task_6',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 12),
    catchup=False,
) as dag:
    for i in range(30):
        if i < 10:

            task = BashOperator(
                task_id=f'echo_{i}',
                env={'NUMBER': i},
                bash_command=f'echo $NUMBER'
            )

            task.doc_md = """
                # BashOperator task documentation
                Running command `echo {i}`
                *i* is **task** number
            """

        else:
            task = PythonOperator(
                task_id=f'print_task_{i}',
                python_callable=print_i,
                op_kwargs={'task_number': i}
            )

            task.doc_md = """
                # PythonOperator task documentation
                Running command `print('task number is: {task_number}')`
                *task_number* is **task** number
            """

        task
