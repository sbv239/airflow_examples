from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


def var():
    from airflow.models import Variable
    return print(Variable.get("is_startml"))


with DAG('hw_10_vorotnikov', default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}, start_date=datetime(2022, 3, 20), catchup=False) as dag:
    t1 = PythonOperator(task_id='print_variable', python_callable=var)
