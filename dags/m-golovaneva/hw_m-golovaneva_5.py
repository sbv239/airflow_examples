from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG("hw_m-golovaneva_task5",

    default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
    },
    description='my DAG for 5th task Lecture 11',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 8, 21),
    catchup=False,

    tags=['task5_L11']

         ) as dag:

    # BashOperator оператор должен  использовать шаблонизированную команду следующего вида:
    # "Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts и затем распечатать значение run_id".
    # Здесь ts и run_id - шаблонные переменные
    templated_command = dedent("""
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
    """) # шаблонизация через Jinja https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html

    t1 = BashOperator(
        task_id="task1",
        bash_command=templated_command
    )

    t1