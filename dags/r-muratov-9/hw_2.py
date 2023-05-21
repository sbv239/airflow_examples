from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow import DAG

with DAG(

    'hw_2_r-muratov-9',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    schedule_interval=timedelta(days=1),

    start_date=datetime(2023,5,20),

    catchup=False
) as dag:
    
    t1 = BashOperator(
        task_id='print_directory_path',
        bash_command='pwd',
    )
    
    def print_ds(ds):

        print(ds)

    t2 = PythonOperator(
        task_id='',
        python_callable=print_ds
    )


    t1 >> t2

