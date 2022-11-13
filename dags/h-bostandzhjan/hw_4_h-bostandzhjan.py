"""
### a simple tutorial dag for printing
"""
from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

default_args = {
    'depend_on_pas': False,
    'email':['airflow@exampl.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries':1,
    'retry_delay':1,
    'retry_delay':timedelta(minutes=5),
}
with DAG(
        'hw_2_h-bostandzhjan',
        default_args=default_args,
        description='A simple tutorial DAG',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021,1,1),
        catchup=False,
        tags=['hristo']
        ) as dag:

    templated_command = dedent(
        """
    {% for i in range(5)#}
        echo "{{ ts}}"
        echo "{{ rin_id }}"
    {% endfor %}
    """
    )
    t1= BashOperator(
        task_id='print_info',
        bash_command=templed_command,
    )
    t1
    t1.doc_md = dedent(
        'print templated command'
    )

    dag.doc_md = __doc__

    t1
