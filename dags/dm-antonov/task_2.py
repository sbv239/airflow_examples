from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_2_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_2']
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )


    def print_args(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'string for log'


    t2 = PythonOperator(
        task_id='print_args',
        python_callable=print_args
    )
