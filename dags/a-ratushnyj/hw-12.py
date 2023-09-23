"""
Test documentation
"""
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

def get_variables():
    print(Variable.get('is_startml'))

with DAG(
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5)},

        start_date=datetime(2023, 9, 22),
        dag_id="hw_12_a-ratushnyj",
        schedule_interval=timedelta(days=1),
        tags=['hw-12'],

) as dag:

    get_variable =  PythonOperator(
        task_id = 'get_var',
        python_callable=get_variables
    )
    get_variable