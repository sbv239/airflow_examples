from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {'depends_on_past': False,
                'email': ['airflow@example.com'],
                'email_on_failure': False,
                'email_on_retry': False,
                'retries': 1,
                'retry_delay': timedelta(minutes=5),
                }

with DAG(
    'hw_j-kutnjak-21_9',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 6, 30),
    catchup=False,
    tags=['example'],
) as dag:

    def put_ti(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )
    def get_ti(ti):
        ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='hw_j-kutnjak-21_9_put'
        )

    t1 = PythonOperator(
        task_id='hw_j-kutnjak-21_9_put',
        python_callable=put_ti
    )

    t2 = PythonOperator(
        task_id='hw_j-kutnjak-21_9_get',
        python_callable=get_ti
    )

    t1 >> t2


