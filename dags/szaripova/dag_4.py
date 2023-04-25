from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
        dag_id='hw_szaripova_3',
        default_args=default_args,
        description='DAG for Step 3',
        start_date=datetime(2023, 4, 25)
) as dag:

    tasks = []
    for i in range(10):
        tasks.append(
            BashOperator(
                task_id=f'hw_szaripova_3_bash_{i}',
                bash_command=f'echo {i}'
            )
        )

    def print_task_number(task_number):
        print("task number is: {task_number}")

    for i in range(20):
        tasks.append(
            PythonOperator(
                task_id=f'hw_szaripova_3_python_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i}
            )
        )

    for j in range(1, len(tasks)):
        tasks[j - 1] >> tasks[j]
