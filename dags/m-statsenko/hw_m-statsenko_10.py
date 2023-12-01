from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python import PythonOperator


def get_xcom_push(ti):
    return "Airflow tracks everything"


def get_xcom_pull(ti):
    print(ti.xcom_pull(key='return_value', task_ids='xcom_push'))


default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
        'hw_10_m-statsenko',
        default_args=default_args,
        description='HW 10',
        start_date=datetime(2023, 11, 30),
        catchup=False,
        tags=['HW 10']
) as dag:
    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=get_xcom_push
    )
    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=get_xcom_pull

    )

    t1 >> t2
