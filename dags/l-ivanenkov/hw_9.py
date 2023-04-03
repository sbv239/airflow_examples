from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
        'hw_9_l-ivanenkov',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='hw_9_l-ivanenkov',
        start_date=datetime(2022, 1, 1),
        catchup=False,
        tags=['hw_9_l-ivanenkov'],
) as dag:
    def add_to_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )

    def get_from_xcom(ti):
        print(ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='xcom_add'
        ))

    t1 = PythonOperator(
        task_id='xcom_add',
        python_callable=add_to_xcom,
    )

    t2 = PythonOperator(
        task_id='xcom_get',
        python_callable=get_from_xcom,
    )

    t1 >> t2
