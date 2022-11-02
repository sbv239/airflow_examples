from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_3_n-anufriev',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},
        description='anufriev_lesson3',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 10, 28),
        catchup=False,
        tags=['hw_3_n-anufriev']
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f'task_number_is_{i}',
            bash_command=f'print({i})')


    def print_task_number(task_number):
        print(f"task number is: {task_number}")


    for j in range(20):
        t2 = PythonOperator(
            task_id=f'print_task_num_{j}',
            python_callable=print_task_number,
            op_kwargs={'task_number': j})

    t1 >> t2
