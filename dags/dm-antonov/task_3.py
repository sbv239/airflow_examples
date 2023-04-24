from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_3_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_3']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='echo' + str(i),
            bash_command=f'echo {i}'
        )


    def print_task(task_number):
        print(f'task number is: {task_number}')
        return 'string for log'


    for task_number in range(20):
        t2 = PythonOperator(
            task_id='print_task_number' + str(task_number),
            python_callable=print_task,
            op_kwargs={'random_base': float(task_number) / 40}
        )

        t1 >> t2
