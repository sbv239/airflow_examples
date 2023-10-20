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
    description='Print ds and directory',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 20),
    catchup=False,
    tags=['task_2', 'lesson_11'],
) as dag:
    t1 = BashOperator(
        task_id='hw_j-rzayev_1',
        depends_on_past=False,
        bash_command='pwd'
    )

    t2 = PythonOperator(
        task_id='hw_j-rzayev_2',
        python_callable=print_ds
    )

    t1 >> t2
