"""
PythonOperator and xcom
"""
from datetime import datetime, timedelta
# from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



with DAG(
    'hw_9_i-daniljuk',
    # Параметры по умолчанию для тасок
    default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
    },
    description='A cycle tasks DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_7_i-daniljuk'],
) as dag:
    

    def push_data(ti):
        """
        Gets test value
        """
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def pull_data(ti):
        """
        Evaluates testing increase results
        """
        pull_result = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='xcom push'
        )
        print(pull_result)

    opr_get_covid_data = PythonOperator(
        task_id = 'get_data',
        python_callable=push_data,
    )
    opr_analyze_testing_data = PythonOperator(
        task_id = 'pull_data',
        python_callable=pull_data,
    )

    opr_get_covid_data >> opr_analyze_testing_data