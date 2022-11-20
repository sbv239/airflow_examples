from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}


def print_line(task_number):
    print(f"task number is: {task_number}")


with DAG(
        dag_id="task3_v-saharov-20",
        default_args=default_args,
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 1, 1),
        catchup=False
) as dag:
    for value in range(1, 31):
        if value < 11:
            bash_tasks = BashOperator(
                task_id=f"echo_{value}",
                bash_command=f"echo {value}",
                doc_md=dedent("""
                this `code` is _used_ **in this text**

                #added task

                """)
            )

        else:
            python_tasks = PythonOperator(
                task_id=f"echo_python{value}",
                python_callable=print_line,
                op_kwargs={'task_number': value},
                doc_md=dedent("""
                this `code` is _used_ **in this text**

                #added task

                """)
            )
