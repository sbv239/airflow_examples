import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta



def get_ds(ds, **kwargs):
    print(kwargs)
    print(ds)
    return ds


with DAG(
    'hw_p-matchenkov_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task_2_dag',
    #schedule_interval=timedelta(days=1)
    start_date=datetime.datetime(2023, 10, 16),
    catchup=False,
    tags=['matchenkov_2']
) as dag:

    t1 = BashOperator(
        task_id='print_pwd',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_execution_time',
        python_callable=get_ds
    )

    t1 >> t2