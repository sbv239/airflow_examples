from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

with DAG(
    'e_bogomolova_step_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_7',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_7'],
) as dag:

    def print_task_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(20):
        t1 = PythonOperator(
            task_id='task_number_' + str(i),
            python_callable=print_task_number,
            op_kwargs={'task_number': i},
        )
        t1
