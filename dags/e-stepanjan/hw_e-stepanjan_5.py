from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_e-stepanjan_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My fifth training DAG',
        start_date=datetime(2023, 6, 20),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['stepanjan', 'step_5']
) as dag:

    templated_command = dedent(
        '''
        {% for i in range (5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        '''
    )

    t1 = BashOperator(
        task_id='ts_run_id_printing',
        bash_command=templated_command
    )
