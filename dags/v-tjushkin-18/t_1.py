from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'v-tjushkin-18_t1',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Lesson 11 (Task 1)',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 5, 16),
    catchup=False,
) as dag:
    t1 = BashOperator(
        task_id='t1_pwd',
        bash_command='pwd'
    )

    def print_ds(ds):
        print(ds, 'its work')

    t2 = PythonOperator(
        task_id='t2_print',
        python_callable=print_ds
    )

    t1 >> t2