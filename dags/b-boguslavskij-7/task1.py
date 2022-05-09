from datetime import timedelta, datetime

from airflow import DAG
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
            'retry_delay': timedelta(minutes=5),
        },

        description='HW 1 DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 17),
        catchup=False,

) as dag:
    def print_date(ds, **kwargs):
        print(ds)


    run_bash = BashOperator(
        task_id='current_working_directory',
        bash_command='pwd'
    )

    run_python = PythonOperator(
        task_id='print_date',  # id, будет отображаться в интерфейсе
        python_callable=print_date
    )

    run_bash >> run_python

