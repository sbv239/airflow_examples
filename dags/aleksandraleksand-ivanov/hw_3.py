from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

# aleksandraleksand-ivanov
default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_info(*args):
    return f"task number is: {args}"


with DAG(
        dag_id="hw_aleksandraleksand-ivanov_3",
        default_args=default_args,
        start_date=datetime(2023, 9, 18),
        schedule_interval=timedelta(days=1)
) as dag:
    for i in range(10):
        task_bash = BashOperator(
            task_id=f"bash_print_{i}",
            bash_command=f"echo {i}"

        )
    for i in range(20):
        task_python = PythonOperator(
            task_id=f"python_task_print_{i}",
            python_callable=print_info,
            op_args=[i]
        )

    task_bash >> task_python
