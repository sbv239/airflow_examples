from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

from airflow import DAG


def print_context(task_number):
    """Пример PythonOperator"""
    # Через синтаксис **kwargs можно получить словарь
    # с настройками Airflow. Значения оттуда могут пригодиться.
    # Пока нам не нужно
    # В ds Airflow за нас подставит текущую логическую дату - строку в формате YYYY-MM-DD
    print(f'task number is {task_number}')
    return 'Whatever you return gets printed in the logs'




with DAG(
    'hw_5_de-jakovlev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{run_id}}"
    {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id=f'print_variables',
        bash_command=templated_command,
    )








