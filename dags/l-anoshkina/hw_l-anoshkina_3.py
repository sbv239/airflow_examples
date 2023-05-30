from airflow import DAG
from datetime import timedelta, datetime

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_l-anoshkina_3',

        default_args={
        'depends_on_past': False,
                           'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description = 'HomeWork task2',
        schedule_interval = timedelta(days=1),
        start_date = datetime(2023, 5, 29),

        catchup = False,

        ) as dag:

        for i in range(10):
                task = BashOperator(
                        task_id = 'print' + str(i),
                        bash_command = "echo"+ str(i)
                )

        def print_task_number(task_number):
                print(f"task number is: {task_number}")

        for i in range(10, 30):
            task = PythonOperator(
                    task_id = 'task_number'+str(i),
                    python_callable = print_task_number,
                    op_kwargs = {'task_number': int(i)},
            )
