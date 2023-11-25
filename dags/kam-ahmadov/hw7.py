from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_kamilahmadov_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='first DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 20),
    catchup=False,
    tags=["hw_7"]
) as dag:


    def print_context(task, ts, run_id, **kwargs):
        print(ts)
        print(run_id)
        print(kwargs)
        print(task)

    for i in range(20):
        t2 = PythonOperator(
            task_id=f"task_{i}",
            python_callable=print_context,
            op_kwargs={"task number is": f"task_{i}"}
        )