from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_task_number(ts, run_id, **kwargs):
    print(f'task number is: {str(kwargs["task_number"])}')
    print(f'ts: {str(ts)}')
    print(f'run_id: {str(run_id)}')

with DAG(
        'rag_hw_6',
    description='HW_step_6',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:
    for i in range(10):
        task = PythonOperator(
            task_id = f'python_task_{i}',
            python_callable=print_task_number,
            op_kwargs = {'task_number': i}
        )
        task
