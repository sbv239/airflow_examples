from airflow import DAG
from airflow.operators.bash import BashOperator, PythonOperator
from datetime import datetime, timedelta


def print_ds(ds):
    print(ds)


with DAG(
        'task_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 9, 11),
        catchup=False,
        tags=['example'],
) as dag:
    t1 = BashOperator(
        task_id='print pwd',
        bash_command='pwd '
    )

    t2 = PythonOperator(
        task_id="print ds",
        python_callable=print_ds
    )

    t1 >> t2
