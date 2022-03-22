from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'first_dag',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
) as dag:
    t1 = BashOperator(
        task_id='print_working_directory',
        bash_command='pwd',
    )


    def print_ds(ds):
        print(ds)


    t2 = PythonOperator(
        task_id='print_the_ds',
        python_callable=print_ds,
    )

    t1 >> t2
