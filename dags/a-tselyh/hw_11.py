from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def print_var():
    from airflow.models import Variable
    is_startml = Variable.get("is_startml")
    print(is_startml) # необходимо передать имя, заданное при создании Variable

with DAG(
        'a-tselyh_var_11',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='this dag prints var',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2021, 3, 20),
        catchup=False,
        tags=['step_11'],
) as dag:
    t1 = PythonOperator(
        task_id='simple_print_var',
        python_callable=print_var,
    )

    t1
