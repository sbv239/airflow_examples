from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_mi-ponomarev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_re            try': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='task_3',
    schedule_interval=timedelta(days=365),
    start_date=datetime(2023, 4, 22),
    catchup=False,
    tags=['task_3']
) as dag:

        for i in range(10):
                task_1 = BashOperator(
                        task_id=f'echo' + str(i),
                        bash_command=f"echo {i}"
                )

        def print_task_number(task_number):
                print(f"task number is: {task_number}")

        for i in range(20):
                task_2 = PythonOperator(
                        task_id=f"print" + str(i),
                        python_callable=print_task_number,
                        op_kwargs={'task_number': i}
                )