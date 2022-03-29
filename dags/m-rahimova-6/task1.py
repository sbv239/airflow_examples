from airflow import DAG
from datetime import timedelta

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'task1',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}
) as dag:

    t1 = BashOperator(
        task_id='print_folder',
        bash_command='pwd',
    )

    def print_date(ds, **kwargs):
        print(ds)
        print('hehe')

    t2 = PythonOperator(
        task_id='print_ds_hehe'
        python_callable=print_date,
    )

    t1 >> t2