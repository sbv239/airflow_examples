from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime
from airflow.models import Variable

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('gladkaja_hw_11',
         default_args=default_args,
         description='A simple tutorial DAG_11',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['gladkaja']
         ) as dag:

    def get_var():
        var = Variable.get("is_startml")
        print(var)

    t1: PythonOperator = PythonOperator(
        task_id="t1_get_var",
        python_callable=get_var)
    t1
