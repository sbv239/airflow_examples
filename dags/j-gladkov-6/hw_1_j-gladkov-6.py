from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import datetime, timedelta

with DAG(
        'hw_1_j-gladkov-6',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description = 'HomeWork',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2022, 1, 21),
        catchup = False,
        tags = ['hw_1']
) as dag:

    t1 = BashOperator(
        task_id = 'execute_pwd'
        bash_command = 'pwd'
    )

    def print_ds(ds):
        print(ds)
        print("hello airflow")

    t2 = PythonOperator(
        task_id = 'print_ds',
        python_callable = print_ds,
    )

    t1 >> t2
