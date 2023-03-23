from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_2_s-plaksin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='Print date and working directory',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 3, 22),
        catchup=False,
        tags='hw_2',
) as dag:
    t1 = BashOperator(
        task_id='print working directory',
        bash_command='pwd',
    )


    def print_ds(ds):
        print(ds)
        return 'Date has printed'


    t2 = PythonOperator(
        task_id='print the date',
        python_callable=print_ds,
    )
    t1 >> t2
