from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_e-stepanjan_9',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='My ninth training DAG',
        start_date=datetime(2023, 6, 20),
        schedule_interval=timedelta(days=1),
        catchup=False,
        tags=['e-stepanjan', 'step_9']
) as dag:
    def zip_xcom_values(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test')


    def unzip_xcom_values(ti):
        result = ti.xcom_pull(key='sample_xcom_key', task_ids='zip_data')
        print(result)


    t1 = PythonOperator(
        task_id='zip_data',
        python_callable=zip_xcom_values
    )
    t2 = PythonOperator(
        task_id='unzip_data',
        python_callable=unzip_xcom_values
    )

    t1 >> t2
