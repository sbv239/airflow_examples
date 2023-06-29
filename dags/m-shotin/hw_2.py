from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'hw_m-shotin_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False, 'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2 DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 6, 28),
        catchup=False,
        tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd',
    )


    def print_the_ds(ds, **kwargs):
        print(kwargs)
        print(ds)
        return 'print_ds'


    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_the_ds,
    )

    t1 >> t2
