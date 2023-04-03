from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
with DAG(
    'a-igumnov_task_5',

    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description = 'hw_5_a-igumnov',
    schedule_interval = timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['hw_5_a-igumnov']


) as dag:

    templated_command = dedent(
        """
        {% for i in range(5) %}
            run_id = i
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        """
    )

    t1 = BashOperator(
        task_id = 'hw_5_templated',
        depends_on_past = False,
        bash_command = templated_command
    )
    
    t1