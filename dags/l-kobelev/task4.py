from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


def print_loop_num(task_number):
    print(f"task number is: {task_number}")
    return 0


with DAG(
        'hw_l_kobelev_4',
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

    dag.doc_md = dedent(
        """
        # `code` and **some bold text** and __italic__
        # test
        """
    )

    task.doc_md = dedent(
        """
        # `code` and **some bold text** and __italic__
        # test
        """
    )