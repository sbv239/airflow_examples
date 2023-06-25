from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from airflow.models import Variable


import psycopg2
from psycopg2.extras import RealDictCursor

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG(
        'hw_11_a-samofalov',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:


    def variable_test():
        print(Variable.get('is_startml'))
        return "ok"

    dag_variable_samofalov = PythonOperator(
        task_id='db_connection_samofalov_hw_11',
        python_callable=variable_test
    )

    dag_variable_samofalov