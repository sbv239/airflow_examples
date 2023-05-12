from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'ratibor_task10',
    start_date=datetime(2023, 5, 11),
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
) as dag:

    def return_str():
        return "Airflow tracks everything"

    def xcom_get(ti):
        xcom_value =  ti.xcom_pull(
            key="return_value",
            task_ids='xcom_setter'
        )
        print(xcom_value)
    

    xcom_setter = PythonOperator(
        task_id='xcom_setter',
        python_callable=return_str
    )

    xcom_getter = PythonOperator(
        task_id='xcom_getter',
        python_callable=xcom_get
    )

    xcom_setter >> xcom_getter
