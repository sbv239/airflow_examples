from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from textwrap import dedent

with DAG(
    'n_muzalev_task6', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='study_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['task_muzalev']
) as dag:
    def twenties(ts, run_id, task_number):
        return f"{ts}, {run_id}"

    for i in range(20):
        t2 = PythonOperator(
            task_id='print_task' + str(i),
            python_callable=twenties,
            op_kwargs = {"task_number": str(i)}
        )

t2