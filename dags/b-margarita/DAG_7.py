from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_b-margarita_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 7 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['b-margarita'],
) as dag:
    def print_task(task_number,ts, run_id, **kwargs):
        print(f"task number is: {task_number}")
        print(ts)
        print(run_id)

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id=f'print_task_{i}',
            python_callable=print_task,
            op_kwargs={'task_number': i}
        )

    t2