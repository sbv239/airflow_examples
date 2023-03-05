from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
    'hw11_2_r-safarov',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='Task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_11_r-safarov']
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'

    run_this = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
    )
