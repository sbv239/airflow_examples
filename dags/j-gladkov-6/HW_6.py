from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'hw_6_j-gladkov-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description = "More Args",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022, 3, 22),
    catchup = False,
    tags = ['hw_6'],
) as dag:

    def print_func(task_number, **kwargs):
        print(f"task number is: {task_number}")
        print(kwargs['ts'])
        print(kwargs['run_id'])



    for i in range(1, 31):
        if i < 11:
            task_bash = BashOperator(
                task_id = "echo_" + str(i),
                bash_command = templ_comma,
                env={"NUMBER": str(i)}
            )
        else:
            task_pyth = PythonOperator(
                task_id = 'task_' + str(i),
                python_callable = print_func,
                op_kwargs={'task_number': i},
            )
