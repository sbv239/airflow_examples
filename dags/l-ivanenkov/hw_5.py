from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_5_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_5_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_5_l-ivanenkov'],
) as dag:

    task = dedent(
        '''
        {% for i in range (5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        '''
    )

    t1 = BashOperator(
        task_id='task',
        bash_command=task
    )
