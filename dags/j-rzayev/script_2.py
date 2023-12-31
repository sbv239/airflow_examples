from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_da(ds, **kwargs):
    print("This is ds date")
    print(ds)
    return 'Ok'


with DAG(
    'j-rzayev_task_2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
   },
    description='task_2',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['task_2', 'lesson_11', 'j-rzayev'],
) as dag:
    t1 = BashOperator(
        task_id='print_path',
        depends_on_past=False,
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='print_ds',
        python_callable=print_da
    )

    t1 >> t2
