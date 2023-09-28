from datetime import timedelta, datetime

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_loop_num(ts, run_id, task_number):
    print(ts, run_id)
    print(f"task number is: {task_number}")
    return 0


with DAG(
        'hw_l_kobelev_7',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='DAG hw2',
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:

    for i in range(10):
        task = BashOperator(
            task_id='bash_op_' + str(i),
            bash_command=f'echo {i}',
        )

    for i in range(20):
        task = PythonOperator(
            task_id='python_op_' + str(i),
            python_callable=print_loop_num,
            op_kwargs={'task_number': i}
        )