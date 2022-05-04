from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator

with DAG(
    "hw_2_v-baev-7",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='loops in DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=5, day=3),
    catchup=False,
    tags=['hw_2'],
) as dag:
        for i in range(10):
                task_b = BashOperator(
                task_id='print'+str(i),
                bash_command=f"echo {i}"
        )

def print_context(task_number):
        print("task number is: {task_number}")
for i in range(20):
        task_p = PythonOperator(
                task_id='print_the_context' + str(i),
                python_callable=print_context,
                op_kwargs={'task_number': i}
)
task_b >> task_p

task_b.doc_md = dedent(
        """
        ### _Task B_ documentation
        This task call command `print` in `console`.
        """
)

task_p.doc_md = dedent(
        """
        # **Task P** documentation
        This task call the same command, as _Task B_, but with Python.
        """
)