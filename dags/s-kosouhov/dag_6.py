from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_task_number(ts, run_id, **kwargs):
    print("task number is:" + str(kwargs["task_number"]))
    print("ts: " + str(ts))
    print("run_id: " + str(run_id))

with DAG(
    'lesson11_s-kosouhov_task_6_v2',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['example_2'],
) as dag:

    for i in range(5):        
        task = PythonOperator(
            task_id = f"python_task_{i}",
            python_callable=print_task_number,
            op_kwargs = {'task_number': i}
        )
        task
    