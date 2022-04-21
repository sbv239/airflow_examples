import pwd
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

def print_ds(ds):
    print(ds)
    return "good"

with DAG(
    'n_muzalev_task1', 
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='study_DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['task_muzalev']
) as dag:
    t1 = BashOperator(
        task_id='your_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_DS',
        python_callable=print_ds
    )

    t1>>t2