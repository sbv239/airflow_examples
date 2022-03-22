from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='First DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task2'],
) as dag:
    print_pwd = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )


    def print_date(ds):
        print(ds)


    print_ds = PythonOperator(
        task_id='print_ds',
        python_callable=print_date,
    )

    print_pwd >> print_ds
