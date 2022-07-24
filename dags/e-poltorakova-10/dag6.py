from airflow import DAG
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
    'hm_7_e-poltorakova',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['second_dag'],

) as dag:

    def print_number_task(ts, run_id, task_number, **kwargs):
        print(ts)
        print(run_id)
        print(task_number)
                
    for i in range(20):
        task = PythonOperator(
            task_id = 'python_print_' + str(i),
            python_callable = print_number_task,
            op_kwargs = {'task_number': i},
            )
            
    task