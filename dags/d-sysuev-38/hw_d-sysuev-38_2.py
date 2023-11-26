from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_d-sysuev-38_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='hw_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 25),
    catchup=False,
    tags=["hw_2"]
) as dag:

    def print_ds(ds, **kwargs):
        print(kwargs)
        print(ds)
        return "it's working"

    t1 = BashOperator(
        task_id='command_pwd',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_ds,
    )



    t1 >> t2