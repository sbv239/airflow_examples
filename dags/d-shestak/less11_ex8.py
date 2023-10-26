from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from textwrap import dedent

default_args = {
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),

}
with DAG('hw_d-shestak_8',
         default_args=default_args,
         description='hw_d-shestak_8',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_8_d-shestak']
         ) as dag:

    def xcom_push(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value="xcom test"
        )
        return 'string for log [push]'

    def xcom_pull(ti):
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='xcom_push'
        )
        return 'string for log [pull]'

    t1 = PythonOperator(
        task_id='xcom_push',
        python_callable=xcom_push
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2