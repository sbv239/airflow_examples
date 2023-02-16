from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_5_a-tjurin',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },

        description='Task_5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 2, 16),
        catchup=False,

        tags=['Task_5'],
) as dag:

    temp_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{run_id}}"
        """
    )

    t1 = BashOperator(
        task_id='print_in_template',
        bash_command=temp_command,
    )


    t1
