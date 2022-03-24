from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_1_a-donskoj-5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['example'],
) as dag:

    for i in range(1, 11):
        t1 = BashOperator(
            task_id='print_'+str(i),
            bash_command=f"echo {i}"
        )


    def func(num):
        print(f"task number is: {num}")

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='strange_print_function',
            python_callable=func,
            op_kwargs={'num': i}
        )

    t1 >> t2