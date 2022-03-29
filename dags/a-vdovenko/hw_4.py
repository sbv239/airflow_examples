from datetime import timedelta, datetime
from airflow import  DAG
from airflow.operators.bash import BashOperator
from textwrap import dedent


with DAG(
    'hw_4_a-vdovenko',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5), 
},
    description="Lesson 11 home work 4",
    schedule_interval = timedelta(days=1),
    start_date = datetime(2022,1,1),
    catchup=False,
    tags=['a-vdovenko'],
) as dag:
    bash_command = dedent("""
    {% for i in range(5) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
    """)
    task = BashOperator(
        task_id = 'bash_script',
        bash_command=bash_command
    )

    task