from airflow.operators.python import PythonOperator
from airflow import DAG
from datetime import datetime, timedelta


with DAG(
    'hw_7_i-vekhov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='hw_7_i-vekhov',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 3, 29),
    catchup=False,
    tags=['hw_7_i-vekhov'],
) as dag:
    def print_task_number(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)

    for i in range(20):

        task_python = PythonOperator(
                task_id=f'task_python_{i}',
                python_callable=print_task_number,
                op_kwargs={'task_number': i, 'kwargs': i})
        task_python
