from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator


with DAG(
's-filonov-6_hw4',
default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
},

description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 28),
    catchup=False,
    tags=['learning'],
) as dag:
    
    templated_command = dedent(
            """
            {% for i in range(5) %}
                echo "{{ ts }}"
            {% endfor%}
            echo "{{ run_id }}"

            """
        )


    t1 = BashOperator(
            task_id="templated-step4", 
            bash_command=templated_command,
        )  
    

    t1.doc_md = dedent(
        """\
        Working with templates for bash operator
        """
    )

    t1