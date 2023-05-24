from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        "hw_n-efremov_7",
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description="hw_n-efremov_3 DAG",
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 5, 22),
        catchup=False,
        tags=['second_task'],
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id=f'echo_task{i}',
            bash_command=f'echo {i}',
        )


    def print_context(task_number, ts, run_id, **kwargs):
        print(kwargs)
        print(ts)
        print(run_id)
        return f'task number is: task_number'


    for i in range(20):
        t2 = PythonOperator(
            task_id=f'print_{i}',
            python_callable=print_context,
            op_kwargs={'task_number': i},
        )

    t1 >> t2
