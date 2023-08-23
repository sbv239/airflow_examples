from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime


def print_ds(ds):
    return ds


bash_cmd = 'pwd'

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

dag = DAG(
    "hw_2_e-zastrogina",
    default_args=default_args,
    start_date=datetime(2023, 8, 23),
    catchup=False,
)

print_log_date = PythonOperator(
    task_id="print_log_date",
    provide_context=True,
    python_callable=print_ds,
    dag=dag,
)

print_bash = BashOperator(
    task_id="print_bash_directory",
    bash_command=bash_cmd,
)

print_bash.set_downstream(print_log_date)
