from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_5',
         default_args=default_args,
         description='hw_d-shestak_3',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_5_d-shestak']
         ) as dag:
    templated_command = dedent(
        '''
        {% for i in range(5) %}
            echo '{{ ts }}'
            echo '{{ run_id }}'
        {% endfor %}
        '''
    )
    t1 = BashOperator(
        task_id='print-ts-run_id',
        bash_command=templated_command
    )
    t1.doc_md = dedent(
        '''
        # t1 doc:
        `Bash_operator` print `ts` and `run_id`
        '''
    )