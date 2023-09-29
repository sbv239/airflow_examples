from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator



with DAG(
    '5_step_a-klabukov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:


    comand_for_bash = dedent(
        """
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
            echo "{{ run_id }}"

        """
    )
    t3 = BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=comand_for_bash,
    )
