from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator


def print_arguments(ts, run_id, task_number):
    print(f"task number is: {task_number}")
    print(f'ts:{ts}')
    print(f'run_id:{run_id}')


with DAG(
    'hw_6_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description="Lesson 11 home work 6",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    for i in range(31):
        task = PythonOperator(
            task_id=f'print_arguments_{i}',
            op_kwargs={'task_number': i},
            python_callable=print_arguments
        )

    task
