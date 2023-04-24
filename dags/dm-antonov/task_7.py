from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'task_7_dm-antonov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='task_7',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 4, 23),
        catchup=False,
        tags=['task_7']
) as dag:
    def print_task(task_number, ts, run_id):
        print(ts)
        print(run_id)
        print(f'task number is: {task_number}')
        return 'string for log'


    for i in range(20):
        t1 = PythonOperator(
            task_id='print_task_number' + str(i),
            op_kwargs={'task_number': i},
            python_callable=print_task
        )
