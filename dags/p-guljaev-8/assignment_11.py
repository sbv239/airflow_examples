"""
##Assignment 11 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
        'gul_assignment_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='Variable practice',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dags']
) as dag:
    def get_airflow_variable():
        from airflow.models import Variable

        print(f'The result variable is: {Variable.get("is_startml")}')


    p1 = PythonOperator(
        task_id='get_airflow_variable_task',
        python_callable=get_airflow_variable
    )

    dag.doc_md = __doc__
