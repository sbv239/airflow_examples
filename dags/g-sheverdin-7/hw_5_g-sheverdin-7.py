import os
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'g-sheverdin-7_DAG_task05',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='DAG in 4 step',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 11),
        catchup=False,
        tags=['g-sheverdin-7_task05'],
) as dag:

    def func(num):
        print(f"task number is: {num}")

    for i in range(31):
        if i <= 10:
            os.environ['NUMBER'] = str(i)
            t1 = BashOperator(
                task_id='print_'+str(i),
                bash_command=f"echo $NUMBER"
            )
        else:
            t2 = PythonOperator(
                task_id='task_number_' + str(i),
                python_callable=func,
                op_kwargs={'num': i}
            )

    t1 >> t2
