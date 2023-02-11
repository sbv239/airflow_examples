"""
Step 2
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'petrashov_step_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=1),
        },
        description='step_2 - solution',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 10),
        catchup=False,
        tags=['step_2'],
) as dag:
    def print_ds(ds):
        print(ds)
        return 'Step_2  - def print_ds()'


    t1 = PythonOperator(
        task_id='python_print_ds',
        python_callable=print_ds,
    )

    t2 = BashOperator(
        task_id='bash_pwd',
        bash_command='pwd',
    )

    t2 >> t1
