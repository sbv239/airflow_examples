from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime


def print_date(ds, **kwargs):
    print(ds)
    print(kwargs)
    return 'wow nice'


with DAG(
        'hw_2_d-korjakov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='first DAG with BashOperator and PythonOperator',
        start_date=datetime(2023, 9, 17),
        schedule_interval=timedelta(days=1)
) as dag:
    t1 = BashOperator(
        task_id='get_dir',
        bash_command='pwd',
    )

    t2 = PythonOperator(
        task_id='get_date',
        python_callable=print_date,
    )

    t1 >> t2
