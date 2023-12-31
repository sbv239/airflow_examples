from datetime import datetime, timedelta

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'dynamic_tasks',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='home work "dynamic_tasks"',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 9, 28),
    catchup=False,
    tags=['hw_3'],
) as dag:

    def print_task_num(task_number):
        print(f'task number is: {task_number}')

    for i in range(1, 11):
        t1 = BashOperator(
            task_id='task_' + str(i),
            bash_command=f'echo {i}'
        )

    for i in range(11, 31):
        t2 = PythonOperator(
            task_id='task_' + str(i),
            python_callable=print_task_num,
            op_kwargs={'task_number': i}
        )
        t1 >> t2

    if __name__ == "__main__":
        dag.test()