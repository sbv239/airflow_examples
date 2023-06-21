from airflow import DAG
from datetime import timedelta, datetime
from airflow.operators.bash import BashOperator
from textwrap import dedent

with DAG(
    'task_r-izutov_5',


default_args={

    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
},

    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 20),
    catchup=False,
    tags=['izutov']
) as dag:

    template = dedent(
        '''
        {% for i in range (5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        '''
    )

    print_task = BashOperator(task_id='hw_5_izutov_printing', bash_command=template)
