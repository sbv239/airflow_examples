from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_task_number(task_number):
    print(f"task number is: {task_number}")
    return "task number printed"


with DAG(
        'sibi_t3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='task_3_dag',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 1),
        catchup=False,
        tags=['sibi'],
) as dag:
    for i in range(30):
        if i < 10:
            task_1 = BashOperator(
                task_id=f"echo_task_number {i}",
                bash_command=f"echo {i}"
            )
        else:
            task_2 = PythonOperator(
                task_id='print_task_number: ' + str(i),
                python_callable=print_task_number,
                op_kwargs={'task_number': i},
            )

            task_1 >> task_2