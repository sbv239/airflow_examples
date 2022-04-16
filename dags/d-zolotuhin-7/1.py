from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
    'hw_1_d-zolotuhin-7',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task_01_d_zolotukhin',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 17),
    catchup=False,
    tags=['task_01_d_zolotukhin'],
) as dag:

    t1 = BashOperator(
        task_id='Bash_pwd',
        bash_command='pwd',
    )

    def python_pr(ds, **kwargs):
        print(ds)
        return None

    t2 = PythonOperator(
        task_id='python_print',
        python_callable=python_pr,
    )

    t1 >> t2
