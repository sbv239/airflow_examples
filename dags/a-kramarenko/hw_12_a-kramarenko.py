from datetime import datetime, timedelta
from airflow import DAG

from airflow.operators.python import PythonOperator

with DAG(
    'hw_a-kramarenko_10',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='lesson 11 task 10 DAG',
    start_date=datetime(2023, 10, 26),
) as dag:
    
    def variable_import():
        from airflow.models import Variable
        print(Variable.get('is_startml'))

    task1 = PythonOperator(
        task_id='task1',
        python_callable=variable_import,
    )

    task1