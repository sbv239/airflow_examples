from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash import BashOperator
with DAG(
    'a-malahov_task2',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malahov task 1',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:

    for i in range(10):
        bach_op = BashOperator(
            task_id='echo_' + str(i),  # id, будет отображаться в интерфейсе
            bash_command=f"echo {i}" , # какую bash команду выполнить в этом таске
        )

    def print_number(task_number):
        print(f"task number is: {task_number}")

    for i in range(10,30):
        pyhton_op = PythonOperator(
            task_id='print_the_number_' + str(i),  # нужен task_id, как и всем операторам
            python_callable=print_number,  # свойственен только для PythonOperator - передаем саму функцию
            op_kwargs={'task_number': i},
        )

    # А вот так в Airflow указывается последовательность задач
    bach_op >> pyhton_op