from datetime import datetime, timedelta


from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent


# def print_da(ds, **kwargs):
#     print("This is ds date")
#     print(ds)
#     return 'Ok'
#

# def print_task(task_number):
#     print(f'task number is: {task_number}')

with DAG(
    'aakulzhanov_task_5',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
   },
    description='A simple Task 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    jinja_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )
    task_1 = BashOperator(
        task_id='shablon_bash',
        depends_on_past=False,
        bash_command=jinja_command
    )
