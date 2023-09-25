from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


def print_var():
    var = Variable.get('is_startml')
    print(var)


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_12_d-korjakov',
        description='variable DAG',
        default_args=default_args,
        start_date=datetime(2023, 9, 24),
        schedule_interval=timedelta(days=1),
) as dag:
    t1 = PythonOperator(
        task_id='variable_test',
        python_callable=print_var,
    )

    t1
