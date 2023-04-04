from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
    'task_5_stryuk',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='dag for task 5',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023,4,2),
    catchup=False,
    tags=['task_5', 'stryuk']
) as dag:
        for_echo = dedent(
                """
                {% for i in range(5) %}
                        echo "{{ ts }}"
                        echo "{{ run_id }}"
                {% endfor %}
                """
        )

        t1 = BashOperator(
                task_id='for_echo',
                bash_command=for_echo
        )

        t1
