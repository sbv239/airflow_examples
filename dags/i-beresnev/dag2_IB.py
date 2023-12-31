from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    'HW_3_Beresnev',
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
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'print_smth_{i}',
            bash_command=f"echo {i}"
        )

    def print_context(ds, **kwargs):
        print(f"task number is: {kwargs.get('task_number')}")
        return 'Check logs x2'


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_tn_{i}',
            python_callable=print_context,
            op_kwargs={"task_number": i}
        )
    t1 >> t2