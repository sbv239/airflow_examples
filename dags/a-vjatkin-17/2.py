from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 0


with DAG(
    'tutorial_avvyatkin',
    default_args=default_args,
    description='test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 11),
    catchup=False
) as dag:

    task1 = BashOperator(
        task_id='exec_current_directory',
        bash_command='pwd',
    )

    task2 = PythonOperator(
        task_id='printing_variable_ds',
        python_callable=print_context
    )

    task1 >> task2
