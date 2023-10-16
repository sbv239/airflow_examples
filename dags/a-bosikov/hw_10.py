from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
from textwrap import dedent


def xcom_task_1(ti):
    return "Airflow tracks everything"


def xcom_task_2(ti):
    print(ti.xcom_pull(key="return_value", task_ids='xcom_pusher'))

default_args={
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'hw_10_a-bosikov',
    default_args = default_args,
    start_date = datetime.now(),
    tags=['a-bosikov']
)

t1 = PythonOperator(
    task_id='xcom_pusher',
    python_callable=xcom_task_1,
    dag = dag
)

t2 = PythonOperator(
    task_id='xcom_puller',
    python_callable=xcom_task_2,
    dag = dag
)

t1 >> t2