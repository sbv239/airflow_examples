from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent

with DAG(
        'hm_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='первая работа с DAG',
        start_date=datetime(2023, 2, 13)
) as dag:
    templated_command = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        '''
    )
    t1 = BashOperator(
        task_id='hm_5',
        bash_command=templated_command

    )