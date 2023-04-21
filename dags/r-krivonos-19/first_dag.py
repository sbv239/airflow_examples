from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
        'ex_2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My first DAG',
        #schedule_interval=timedelta(days=1),
        start_date=(2023, 4, 22),
        catchup=False,
        tags=['homework'],
) as dag:

    t1 = BashOperator(
        task_id='print_directory',
        bash_command='pwd '
    )
    def know_date(ds, **kwargs):
        print(ds)

    t2 = PythonOperator(
        task_id='actual_date',
        python_callable=know_date,
    )

    t1 >> t2