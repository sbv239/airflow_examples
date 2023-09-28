from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

with DAG(
    'hw_g-vinokurov_6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
        description='DAG in task_2',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 9, 28),
        catchup=False,
) as dag:

    for i in range(10):
        operator_1 = BashOperator(
            task_id=f'Bash_operator_{i}',
            bash_command=f"Count BashOperator: {i}",
        )

    def print_context(ts, run_id, task_number):
        print(f"task number is: {task_number}")
        print(f'ts equals {ts}')
        print(f'run_id equals {run_id}')

    for i in range(20):
        operator_2 = PythonOperator(
            task_id=f'Python_operator_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

    operator_1 >> operator_2