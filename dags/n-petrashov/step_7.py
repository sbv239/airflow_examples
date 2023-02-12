"""
Step 7
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'petrashov_step_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='step_7 - solution',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['step_7'],
) as dag:
    def print_task_number(ts, run_id, task_number):
        print(f"ts is: {ts}")
        print(f"run_id is: {run_id}")
        print(f"task number is: {task_number}")
        return 'Step_7  - def print_task_number()'


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
