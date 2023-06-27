from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
with DAG(
    'hw_a-shulga-4_1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
    }
    ) as dag:
        t1 = BashOperator(
                task_id = 'print_current_directory'
                bash_command = 'pwd',
        )

        def print_context(ds, **kwargs):
                print(ds)
                print(kwargs)
                return 'Whatever you return gets printed in the log'

        t2 = PythonOperator(
                task_id = 'print_ds',
                python_callable=print_context,

        )

        t1 >> t2