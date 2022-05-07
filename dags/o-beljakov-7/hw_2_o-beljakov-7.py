from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

with DAG(
        'hw_2_o-beljakov-7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 8),
        catchup=False,
        tags=['beljakov']
) as dag:
    for task_number in range(1, 11):
        task = BashOperator(
            task_id=f'hw_2_o-beljakov-7_{task_number}',
            bash_command=f"echo {task_number}"
        )

    def print_task_number(task_number):
        print(f'task number is : {task_number}')
        return 'i printed'

    for task_number in range(11, 31):
        task = PythonOperator(
            task_id=f'hw_2_m-beljakov-7_{task_number}',
            python_callable=print_task_number,
            op_kwargs={'task_number': task_number}
        )

