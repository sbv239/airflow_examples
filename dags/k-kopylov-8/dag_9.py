from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
}

with DAG('kkopylov_dag_9',
    default_args = default_args,
    description='A simple tutorial DAG№9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False) as dag:

    

    def xcom_push_hidden(ti):
         return "Airflow tracks everything"

    def xcom_pull_output(ti):
        from_xcom = ti.xcom_pull(key = 'return_value', task_ids = 't1_xcom_push_hidden')
        print(from_xcom)
        

    t1 = PythonOperator(
        task_id = "t1_xcom_push_hidden",
        python_callable = xcom_push_hidden)
    t2 = PythonOperator(
        task_id = "t2_xcom_pull",
        python_callable = xcom_pull_output)
    
    t1 >> t2
         
         
