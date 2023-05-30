"""
"Для каждого i в диапазоне от 0 до 5 не включительно распечатать значение ts
и затем распечатать значение run_id".
Здесь ts и run_id - это шаблонные переменные
(вспомните, как в лекции подставляли шаблонные переменные).

"""
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_ko-popov_5',
    default_args={
        'depends_on_past': False,
        'email': {'mdkonstantinp@gmail.com'},
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='hw_5_ko-popov dag',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 5, 29),
    catchup=False,
    tags = ['hw_5_ko-popov'],
) as dag:
    task = BashOperator(
        task_id = "hw_5",
        bash_command = dedent(
            """
            {% for i in range(5) %}
                echo "{{ ts }}"
                echo "{{ run_id}}"
            {% endfor %}
            
            """
        )
    )