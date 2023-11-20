from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
    'hw_s-almasjan_2',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },

    start_date = datetime(2023, 11, 20)

) as dag:
    
    def show_date(ds):
        print(ds)
    
    t1 = BashOperator(
        task_id = 'ls',
        bash_command = 'pwd'
    )

    t2 = PythonOperator(
        task_id = 'date',
        python_callable = show_date
    )

    t1 >> t2