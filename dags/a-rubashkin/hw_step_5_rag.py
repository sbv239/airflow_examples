from datetime import timedelta, datetime
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


def print_task_number(task_number):
    print(f'task number is: {task_number}')


with DAG(
    'rag_hw_5',
    description='HW_step_5',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 4, 1),
    catchup=False,
    tags=['rag23'],
) as dag:

    task = BashOperator(
        task_id=f'bash_template_task',
        bash_command=dedent(
            """
            {% for i in range(5) %}
                echo "{{ts}}"
                echo "{{run_id}}"
            {% endfor %}
            """)
    )
    task
