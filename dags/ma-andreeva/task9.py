
from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable

with DAG(
    'task_9_andreeva',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='A DAG for task 9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 30),
    catchup=False,
    tags=['task9','task_9','andreeva'],
) as dag:

    def return_in_xcom ():
        return "Airflow tracks everything"

    def get_xcom(ti):
       print(ti.xcom_pull(key="return_value", task_ids="return_in_XCom"))


    t1 = PythonOperator(
        task_id='return_in_XCom',
        python_callable=return_in_xcom,
    )

    t2 = PythonOperator(
        task_id='pull_from_XCom',
        python_callable=get_xcom,
    )

    t1 >> t2

