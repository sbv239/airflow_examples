from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG('gladkaya_hw_9',
         default_args=default_args,
         description='A simple tutorial DAG№9',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2021, 1, 1),
         catchup=False,
         tags=['gladkaja']
         ) as dag:

    def return_something():
        return "Airflow tracks everything"


    def xcom_pull_return(ti):
        ti.xcom_pull (
            task_ids="return_something",
            key='return_value' )


    t1 = PythonOperator(
        task_id="return_something",
        python_callable=return_something
    )
    t2 = PythonOperator(
        task_id="xcom_pull_return",
        python_callable=xcom_pull_return)

    t1 >> t2




