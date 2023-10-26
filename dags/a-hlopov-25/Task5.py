from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'hw_5_a-hlopov-25',  
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  
        },

        description='Task 5',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2023, 10, 25),
        catchup=False,

        tags=['task 5'],
) as dag:

    template_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{run_id}}"
        """
    )
    t1 = BashOperator(
        task_id='task1',
        bash_command=template_command, 
    )
    t1
