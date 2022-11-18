from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

def print_ds(ds):
    print(ds)

with DAG(
    "task_2",
    default_args=default_args,
    schedule_interval=None
) as dag:
    bash_operator = BashOperator(
        task_id="bash",
        bash_command="pwd",
        dag=dag
    )

    python_operator = PythonOperator(
        task_id="python",
        python_callable=print_ds,
        dag=dag
    )

    bash_operator >> python_operator