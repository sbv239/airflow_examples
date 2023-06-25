from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}


def push_function(ti):
    push_value = "xcom test"
    ti.xcom_push(
        key="sample_xcom_key",
        value=push_value
    )
    return 'push ok', push_value


def pull_function(ti):
    pull_value = ti.xcom_pull(
        key="sample_xcom_key",
        task_ids='push_xcom_samofalov'
    )
    print(pull_value)
    return 'pull ok', pull_value


with DAG(
        'hw_8_a-samofalov',
        start_date=datetime(2021, 1, 1),
        max_active_runs=2,
        schedule_interval=timedelta(minutes=30),
        default_args=default_args,
        catchup=False
) as dag:
    xcom_push_samofalov = PythonOperator(
        task_id='push_xcom_samofalov',
        python_callable=push_function
    )
    xcom_pull_samofalov = PythonOperator(
        task_id='pull_xcom_samofalov',
        python_callable=pull_function
    )

    xcom_push_samofalov >> xcom_pull_samofalov
