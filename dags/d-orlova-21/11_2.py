from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
with DAG (
    'hw_d-orlova-21_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'dag for lesson 11.2',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 6, 28),
    catchup = False
) as dag:
    t1 = BashOperator(
        task_id='pwd',
        bash_command='pwd'
    )
    def print_ds(ds):
        print('date is')
        print(ds)

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds
    )
    t1 >> t2