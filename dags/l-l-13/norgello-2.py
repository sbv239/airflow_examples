from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def printi(**kwargs):
    return print(f"task number is: {task_number}")

temp=dedent("""
    {% for i in range(10)%}
    echo "{{i}}"
    {% endfor %}
    """)
with DAG(
    'norgello-2',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
    description='first task in lesson №11',
    schedule_interval=timedelta(days=3650),
    start_date=datetime(2022, 10, 20),
    catchup=False
) as dag:
    m1=BashOperator(
        task_id='int',
        bash_command=temp)
    for s in range(20):
        m2=PythonOperator(
            task_id=f'number_of_tasks_on_python{s}',
            python_callable=printi)
m1>>m2