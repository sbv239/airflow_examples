from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

with DAG(
        'romanov_3',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_2_b-romanov-14',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 11, 11),
        catchup=False,
        tags=['b-b']

) as dag:

    templated_command = dedent(
    """
    {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
    {% endfor %}
    """
    )
    t3 = BashOperator(
        task_id='the_templated',
        depends_on_past=False,
        bash_command=templated_command,
    )