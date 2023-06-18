from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
    'hw_a-chernova-21_4',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    },
    description='A DAG wyth template',
    schedule_interval=timedelta(days=1),
    start_date+=datetime(2023, 6, 17),
    catchup=False,
    tags=['example'],
) as dag:
    
    
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ params.run_id }}"
        {% endfor %}
        """
    )
    
    t1 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
        params={'run_id': i},
        dag=dag
    )
    
