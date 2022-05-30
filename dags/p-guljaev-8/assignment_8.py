"""
##Assignment 8 DAG documentation
"""
from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
        'gul_assignment_8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)
        },
        description='XCom practice',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 28),
        catchup=False,
        tags=['gul_dag_6']
) as dag:
    # Push the value to XCom
    def push_value_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value="xcom test"

        )


    p1 = PythonOperator(
        task_id='pushing_task',
        python_callable=push_value_xcom
    )


    # Pull the value from XCom and print it
    def pull_value_xcom(ti):
        pulled_value = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids="pushing_task"
        )
        print(f'The input value is: {pulled_value}')


    p2 = PythonOperator(
        task_id='pulling_task',
        python_callable=pull_value_xcom,
    )
    p1 >> p2

    dag.doc_md = __doc__
