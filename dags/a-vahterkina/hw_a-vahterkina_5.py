from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

def print_task_num(task_number):
    print(f'task_num is {task_number}')
    return 'Whatever you return gets printed in the logs'


with DAG(
        'hw_5_a-vahterkina',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_a-vahterkina_5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 17),
        catchup=False,
        tags=['hw_5_a-vahterkina']
) as dag:

    templated_command = dedent(
        """
        {% for i in range(5)%}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        """
    )

    t = BashOperator(
            task_id='hw_5_a-vahterkina',
            bash_command=templated_command
            )

