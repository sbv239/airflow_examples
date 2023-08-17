from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
from datetime import timedelta, datetime

def print_ds(ds):
    print(ds)

with DAG(
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
        task_id='hw_2_i-ildar-23_bash',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='hw_2_i-ildar-23_python',
        python_callable=print_ds,
    )