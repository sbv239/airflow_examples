
"""
Test documentation
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from textwrap import dedent

from airflow.operators.bash import BashOperator

def print_task_num(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'HW_5_v-patrakeev',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta �� ������ datetime
    },
    start_date=datetime.now(),
) as dag:
	

    templated_command = dedent(
            """
            {% for i in range(5) %}
                echo {{ ts }}
                echo {{ run_id }}
            {% endfor %}
            """
    )

    t1 = BashOperator(
            task_id='hw_5_templates',
            bash_command=templated_command
    )

    t1
