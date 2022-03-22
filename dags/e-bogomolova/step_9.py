from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator


def push_test(**kwargs):
    ti = kwargs['ti']
    ti.xcom_push(value='xcom test', key='sample_xcom_key')


def pull_result(**kwargs):
    ti = kwargs['ti']
    print(ti.xcom_pull(task_ids='xcom_push_test', key='sample_xcom_key'))


with DAG(
    'e_bogomolova_step_9',
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG_for_e_bogomolova_step_9',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 3, 18),
    catchup=False,
    tags=['step_9'],
) as dag:

    push_test_task = PythonOperator(
        task_id='xcom_push_test',
        python_callable=push_test,
    )

    pull_result_task = PythonOperator(
        task_id='xcom_pull_test',
        python_callable=pull_result,
    )

    push_test_task >> pull_result_task
