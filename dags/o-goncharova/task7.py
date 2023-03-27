from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'dag_task_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 25)
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id = 'number_of_string' + str(i),
            bash_command = f"echo {i}",
        )
    def print_task_number(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(ts)
        print(run_id)

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number_is_' + str(i),
            python_callable= print_task_number,
            op_kwargs = {'task_number': i},
        )


    t1 >> t2


