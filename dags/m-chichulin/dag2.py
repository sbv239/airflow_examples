'''
Test
'''
from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'Bash',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_2_m-chichulin',
    schedule_interval = timedelta(days=1),
    start_date = datetime(2023, 4, 16)
) as dag:
        t1 = BashOperator(
        task_id = 'direct',
        bash_command = 'pwd'
        )
        def print_context(ds, **kwargs):
                print(kwargs)
                print(ds)
                return 'Whatever you return gets printed in the logs'
        run_this = PythonOperator(
        task_id = 'print_op',
        python_callable=print_context
        )
        t1 >> run_this