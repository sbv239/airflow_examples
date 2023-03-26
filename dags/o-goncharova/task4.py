from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
with DAG(
    'dag_task_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    start_date = datetime(2023, 3, 26)
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id = 'number_of_string' + str(i),
            bash_command = f"echo {i}",
        )
    def print_task_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(20):
        t2 = PythonOperator(
            task_id = 'task_number_is_' + str(i),
            python_callable= print_task_number,
            op_kwargs = {'task_number': i},
        )
    t1.doc_md = dedent(
        """
        #### This task displays text, using `echo {i}`
        and *loop variable*
        **it can be useful**
        """
    )
    t2.doc_md = dedent(
        """
        #### This task prints number of your task,
        *using function* `print`
        **contains 20 repetitions**
        """
    )


    t1 >> t2