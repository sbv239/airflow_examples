from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


def print_tn(task_number):
    print(f"task_number is: {task_number}")


with DAG(
        'hw_3_d-kizelshtejn-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG for hw_3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 27),
        catchup=False,
        tags=['hw_3']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='print' + str(i),
            bash_command=f"echo {i}",
        )

    for i in range(10, 30):
        t2 = PythonOperator(
            task_id='print_tn' + str(i),
            python_callable=print_tn,
            op_kwargs={'task_number': i}
        )

    t1 >> t2