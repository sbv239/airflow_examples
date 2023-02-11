from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


with DAG(
    'a-vjatkin-17_task_3',
    default_args=default_args,
    description='test DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 2, 11),
    catchup=False
) as dag:

    for i in range(10):
        t1 = BashOperator(
            task_id=f"print_echo_{i}",
            bash_command=f"echo {i}",
        )


    def print_task_number(task_id):
        print(f"task number is: {task_id}")


    for i in range(20):
        t2 = PythonOperator(
            task_id=f"print_task_id_{i}",
            python_callable=print_task_number,
            op_kwargs={'task_id': i},
            )