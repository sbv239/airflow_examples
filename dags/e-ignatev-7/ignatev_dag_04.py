from airflow import DAG
from airflow.operators.bash import BashOperator

from datetime import datetime, timedelta
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG(
    'ignatev_dag_04',
    default_args=default_args,
    start_date=datetime(2022, 4, 15),
    max_active_runs=1,
    schedule_interval=timedelta(days = 1),
) as dag:

    templated_cmd = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ ts }}"
            echo "{{ run_id }}"
        {% endfor %}
        '''
    )
    
    task = BashOperator(
        task_id='templated_task',
        bash_command=templated_cmd,
    )

    task.doc_md = '''
    ### task task documentation

    This task fulfils the requirements for *task 05*, *lesson 13* in **StartML** course
    '''