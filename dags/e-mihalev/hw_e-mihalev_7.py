from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
'hw_e-mihalev_7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple DAG',
    start_date=datetime(2023, 10, 15),
    catchup=False,
    tags=['example']
) as dag:
    def func(task_number, ts, run_id):
        print(f'task number is: {task_number}')
        print(ts)
        print(run_id)
    for j in range(20):
        task2 = PythonOperator(task_id=f'func{j}', python_callable=func, op_kwargs={'task_number':j}, provide_context=True)
    task2