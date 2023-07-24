from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 7, 21),
    catchup=False,
    tags=['homework_2_pavelp'],
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

def print_ds(ds):
    print(ds)
    return 'Whatever you return gets printed in the logs'

run_this = PythonOperator(
    task_id='print_ds',
    python_callable=print_ds,
)

t1 >> run_this