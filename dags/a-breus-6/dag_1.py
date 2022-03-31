from airflow import DAG
from datetime import timedelta
from airflow.operators.bash import BashOperator, PythonOperator

with DAG(
    'HW1',

default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 31),
    catchup=False
) as dag:

    t1 = BashOperator(task_id='pwd', bash_command='pwd')

    def print_ds(ds):
        print("it's logic date")
        print(ds)

    t2 = PythonOperator(task_id='print_ds', python_callable=print_ds)

    t1 >> t2
