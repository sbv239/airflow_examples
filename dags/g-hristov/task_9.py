from datetime import datetime, timedelta
from airflow import DAG
from textwrap import dedent
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_g-hristov_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
    },
    description='task9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 11, 28),
    catchup=False,
) as dag:

    def push_ti(ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )

    def pull_ti(ti):
        ti.xcom_pull(
            key="sample_xcom_key",
            task_ids='hw_g-hristov_9_POpush',
        )
    taskpo1 = PythonOperator(
        task_id='hw_g-hristov_9_POpush',
        python_callable=push_ti,
    )
    taskpo2 = PythonOperator(
        task_id='hw_g-hristov_9_POpull',
        python_callable=pull_ti,
    )


    taskpo1>>taskpo2