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
    print("task number is:" + kwargs.get["task_number"])
    print("ts: " + ts)
    print("run_id: " + run_id)

with DAG(
    'lesson11_s-kosouhov_task_6',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 10),
    catchup=False,
    tags=['example_2'],
) as dag:

    for i in range(30):
        if i < 10:
            task = BashOperator(
                task_id = f"bash_task_{i}",
                bash_command = f"echo {i}"
            )
        else:
            task = PythonOperator(
                task_id = f"python_task_{i}",
                python_callable=print_task_number,
                op_kwargs = {'task_number': i}
            )

        task