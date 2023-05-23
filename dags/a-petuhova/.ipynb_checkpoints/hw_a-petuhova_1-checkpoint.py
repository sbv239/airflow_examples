from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'a-petuhova_task1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='HW step 2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 5, 22),
    catchup=False,
    tags=['a-petuhova'],
) as dag:

    t1 = BashOperator(
        task_id='BashOperator for task 1 step 2 a-petuhova',
        bash_command='pwd',
    )

    def print_context(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'Whatever you return gets printed in the logs'


    t2 = PythonOperator(
        task_id='PythonOperator for task 1 step 2 a-petuhova',
        python_callable=print_context,
    )

    t1 >> t2