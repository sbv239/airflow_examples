from datetime import datetime, timedelta
from textwrap import dedent

# Для объявления DAG нужно импортировать класс из airflow
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


with DAG(
    "task_5_k-zhuravlev",

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

    start_date=datetime.now(),
    tags=["Cool_tag"]
) as dag:

    templated_bash_command = dedent(
        """"
    {% for i in range(5) %}
        echo "{{ ts }}
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    bash_task = BashOperator(
        task_id='Bash_task',
        bash_command=templated_bash_command
    )
    bash_task
