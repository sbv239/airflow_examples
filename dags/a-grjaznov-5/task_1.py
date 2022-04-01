from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        '1_a-grjaznov-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_1_a_grjaznov'],
) as dag:
    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd'
    )


    def func(ds):
        print(ds)
        return

 t2 = PythonOperator(
        task_id='prnt smth!',
        python_callable=func
    )

    t1 >> t2