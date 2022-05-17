from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import date, timedelta, datetime
from textwrap import dedent


with DAG(
        'hw_4_i-litvinov-8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='4_Task_DAG_i-litvinov-8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 16),
        catchup=False,
        tags=['i-litvinov-8']
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
        echo "{{ ts }}"
        echo "{{ run_id }}"
        {% endfor %}
        """
    )
    t = BashOperator(
        task_id='templated_task_for_task_4',
        depends_on_past=False,
        bash_command=templated_command,
        dag=dag,

    )
