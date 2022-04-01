from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'step_2',
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
    def func(q):
        print(f"task number is {q}")
    for i in range(30):
        if i<=9:
            t = BashOperator(
                task_id='print_echo' + str(i),
                bash_command=f"echo{i}"
            )
        else:
            t = PythonOperator(
                task_id='prnt_echo' + str(i),
                python_callable=func(i),
                op_kwargs = {'q':int(i)}
            )
