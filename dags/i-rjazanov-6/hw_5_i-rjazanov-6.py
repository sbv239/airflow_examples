from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        # Название таск-дага
        'hw_5_i-rjazanov-6',

        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime

        },
        description='This is from DAG for Ryazanov to Task 4',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 3, 22),
        catchup=False,
        tags=['task_4']


) as dag:

    def print_task_number(num):
        return print(f"task number is: {num}")

    templated_command = dedent(
        """
    {% for i in range(5) %}
        echo "{{ ts }}"
    {% endfor %}
    echo "{{ run_id }}"
    """
    )
    t1= BashOperator(
        task_id='templated',
        depends_on_past=False,
        bash_command=templated_command,
    )


    t1.doc_md = dedent(
        '''
        ## Documentation
        Print `jinja` *templated* command 
        '''
    )

    t1