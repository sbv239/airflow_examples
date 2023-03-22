from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from textwrap import dedent


with DAG('step5_dag', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)}, start_date=datetime(2023,1,1)) as dag:

    t1 = BashOperator(
        task_id = 'bash_op',
        bash_command = dedent(
            """
             {% for i in range(5) %}
                 echo "{{ ts }}"
                 echo "{{ run_id }}"
             {% endfor %}
            """
        )
    )
