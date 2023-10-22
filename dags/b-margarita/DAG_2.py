from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_b-margarita_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['b-margarita'],
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd',
    )

    def print_date(ds):
        print(ds)
        return 'Whatever you return gets printed in the logs'

    t2 = PythonOperator(
        task_id='print_the_date',
        python_callable=print_date,
    )

    t1 >> t2
