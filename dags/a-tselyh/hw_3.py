from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from datetime import timedelta, datetime


def print_context(task_number):
    print("task number is: {task_number}")



with DAG(
    "hw_3_a-tselyh",
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='DAG for the first homework',
    schedule_interval=timedelta(days=1),
    start_date=datetime(year=2022, month=3, day=22),
    catchup=False,
    tags=['hw_3'],
) as dag:
    t1 = BashOperator(
    for i in range 10:
        task_id='echo_' + str(i)',  # id, будет отображаться в интерфейсе
        bash_command= f'echo {i}',  # какую bash команду выполнить в этом таске
    )

    t2 = PythonOperator(
        for i in range 20:
            task_id="'task_number_' + str(i)",
            python_callable=print_context,
            op_kwargs={'task_number': i }
    )
    t2.doc_md = dedent(
        """\
    # Task with PythonOperator
    This **task** *printing* 10 consecutive numbers
    in the form `task number is: {task_number}`

    """
    t1 >> t2