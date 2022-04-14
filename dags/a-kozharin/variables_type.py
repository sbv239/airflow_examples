from airflow import DAG

from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta


def get_variable():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(type(is_startml))
    print(is_startml == "True")
    return is_startml


# Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'variables_types',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    t1 = PythonOperator(
        task_id='example_variable',
        python_callable=get_variable,
    )
