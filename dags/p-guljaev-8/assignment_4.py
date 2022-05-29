"""
#**Assignment 4 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
        'gul_assignment_4',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='This DAG returns templated variables ts and run_id by using for-loop',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dag_4']
) as dag:
    templated_command = dedent(
        """
        {% for i in range(5) %}
            echo {{ ts }}
            echo {{ run_id }}
        {% endfor %}
        """
    )
    t1 = BashOperator(
        task_id='templated',
        bash_command=templated_command
    )

    dag.doc_md = __doc__
    t1.doc_md = dedent(
        """
        ###**Task 1**
        This task prints logical date (ts) in format `2018-01-01T00:00:00+00:00` and `run_id` 
        of the current DAG run 5 times through *bash*.
        """
    )
