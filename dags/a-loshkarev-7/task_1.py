from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_ds(ds):
    print(ds)
    return f'printed {ds}'


with DAG(
    'aloshkarev_task_1',
    default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 12),
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id='check_directory',
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_date',
        python_callable=print_ds
    )

    t1 >> t2
