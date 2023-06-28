from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
    'hw_7_m-sazonov',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description='hw_7_m-sazonov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 27),
    catchup=False,
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'hw_7_sazonov_taks_{i}',
            bash_command=f'echo $NUMBER',
            env={'NUMBER': i}
        )

    def tasks(ts, run_id, **kwargs):
        print(f"task number is: {kwargs['task_number']}")
        print(f"ts parameter: {ts}")
        print(f"run_id parameter': {run_id}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = f'task_number_is_{i}',
            python_callable=tasks,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
