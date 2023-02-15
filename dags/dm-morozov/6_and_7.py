from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_info(task_number, ts, run_id):
    print(f"task number is: {task_number}")
    print(f'ts is {ts}')
    print(f'run_id is {run_id}')


with DAG(
        '6_and_7_dm-morozov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        start_date=datetime(2023, 2, 14)
) as dag:
    for i in range(10):
        task = BashOperator(
            env={'NUMBER': i},
            task_id=f'cycle_Bash_{i}',
            bash_command='echo $NUMBER'
        )

    for i in range(20):
        task = PythonOperator(
            task_id=f'print_in_cycle_{i}',
            python_callable=print_info,
            op_kwargs={'task_number': i, 'ts': '{{ ts }}', 'run_id': '{{ run_id }}'}
        )

        task
