from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import timedelta, datetime

with DAG(
    'hw_ale-turkin_1',
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    },
    description = 'dag_ex_1',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags = ['example']
    ) as dag:

    def task_print(task_number):
        print(f"task number is: {task_number}")

    for i in range(10):
        task = BashOperator(
            task_id = 'echo_test_al',
            bash_command = 'f"echo {i}"'
        )

    for i in range(20):
        task = PythonOperator(
        task_id = 'print_task_num',
        python_callable = task_print,
        op_kwargs={'task_number': {i}}
        )