"""
##Assignment 5 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.bash import BashOperator

with DAG(
        'gul_assignment_5',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Change enviromental variables in bash by using for-loop',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dag_4']
) as dag:
    for i in range(10):
        t1 = BashOperator(
            task_id='bash_task_' + str(i),
            bash_command='echo $NUMBER',
            env={"NUMBER": i} # Specify env variables taken from the for-loop for bash
        )

    dag.doc_md = __doc__
    t1.doc_md = dedent(
        """
        ###**Task 1**
        This task prints NUMBER taken from the `for`-loop 10 times in *bash*.
        """
    )
