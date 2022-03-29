from datetime import timedelta, datetime
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_4_v-kovalev-6',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        },
    description='First DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_4'],
) as dag:

    templated_command = dedent(
        '''
        {% for i in range(5) %}
            echo "{{ ts }}"
        {% endfor %}
        echo "{{ run_id }}"
        '''
    )

    t1 = BashOperator(
        task_id='bash_print_ts_and_runid',
        bash_command=templated_command
    )

    t1.doc_md = dedent(
        """\
    #### Task Documentation
    This **task** starts bash command print `ts` 5 times and print `run_id` by jinja syntax
    """
    )
