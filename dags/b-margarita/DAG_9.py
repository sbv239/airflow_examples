from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

with DAG(
    'hw_b-margarita_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='Task 9 DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['b-margarita'],
) as dag:

    def push_value(ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )

    t1 = PythonOperator(
        task_id='push_value',
        python_callable=push_value,
    )


    def pull_value(ti):
        value = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids="push_value"
        )
        print(value)

    t2 = PythonOperator(
        task_id='pull_value',
        python_callable=pull_value,
    )


    t1 >> t2