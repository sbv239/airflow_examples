"""
Step 3
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'petrashov_step_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='step_3 - solution',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['step_3'],
) as dag:
    def print_task_number(task_number):
        print(f"task number is: {task_number}")
        return 'Step_3  - def print_ds()'


    for i in range(30):
        if i < 10:
            t2 = BashOperator(
                task_id='bash_' + str(i),
                bash_command=f"echo {i}",
            )
        else:
            t1 = PythonOperator(
                task_id='print_task_number_' + str(i),
                python_callable=print_task_number,
                op_kwargs={"task_number": i}
            )

    t2 >> t1
