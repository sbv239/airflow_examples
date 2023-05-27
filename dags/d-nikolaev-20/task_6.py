from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta


def task_3_python_script(task_number):
    """
    Print i
    """
    print(f'task number is: {task_number}')


# Default settings applied to all tasks
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        'hw_6_d-nikolaev',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id=f'hw_d-nikolaev-20_{i}',
            bash_command="echo $NUMBER",
            env={"NUMBER": str(i)})
    for i in range(20):
        task = PythonOperator(
            task_id=f'hw_d-nikolaev-20_p_{i}',
            python_callable=task_3_python_script,
            op_kwargs={'task_number': i}
            )


