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
with DAG('hw_d-shestak_9',
         default_args=default_args,
         description='hw_d-shestak_9',
         schedule_interval=timedelta(days=1),
         start_date=datetime(2023, 10, 21),
         tags=['hw_9_d-shestak']
         ) as dag:

    def return_string():
        return 'Airflow tracks everything'

    def xcom_pull(ti):
        ti.xcom_pull(
            key='return_value',
            task_ids='return_string'
        )
        return 'string for log [pull]'

    t1 = PythonOperator(
        task_id='return_string',
        python_callable=return_string
    )

    t2 = PythonOperator(
        task_id='xcom_pull',
        python_callable=xcom_pull
    )

    t1 >> t2