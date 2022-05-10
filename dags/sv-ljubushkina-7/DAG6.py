from datetime import datetime, timedelta
import os

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'DAG_6_sv-ljubushkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },

        description='DAG_6_sv-ljubushkina',

        schedule_interval=timedelta(days=1),

        start_date=datetime(2022, 4, 10),

        catchup=False,

        tags=['DAG_6_sv-ljubushkina'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='t1_' + str(i),
            bash_command="echo $NUMBER",
            env={"NUMBER": i}
        )


    def print_context(task_num, ts, run_id):
        print(f'task number is: {task_num}')
        print(ts)
        print(run_id)


    for i in range(20):
        t2 = PythonOperator(
            task_id='task_number' + str(i),
            python_callable=print_context,
            op_kwargs={'task_num': i},
        )

    t1 >> t2

