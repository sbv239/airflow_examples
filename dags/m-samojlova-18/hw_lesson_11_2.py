from datetime import timedelta, datetime
from airflow import DAG

from airflow.operators.bash import BashOperator
# from airflow.operators.python_operator import PythonOperator

from airflow.operators.python import PythonOperator

def print_context(ds, **kwargs):
    print(kwargs)
    print(ds)
    return 'Whatever you return gets printed in the logs'


with DAG(
    'homework',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='hw_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 3, 31),
    catchup=False,
    tags=['11_2'],
) as dag:
    t1 = BashOperator(
        task_id='present_working_directory',
        bash_command='pwd',
    )
    t2 = PythonOperator(
        task_id='print_the_context',
        python_callable=print_context
    )

    t1 >> t2
