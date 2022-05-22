from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

state = 'am'

with DAG(
        'a-malahov_task8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),  # timedelta из пакета datetime
        },
        description='a-malahov task 8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 5, 10),
        catchup=False,
        tags=['malahov'],
) as dag:

    def push_xcom (ti):
        ti.xcom_push(
            key="sample_xcom_key",
            value="xcom test"
        )
    def pull_xcom(ti):
        testing_increases = ti.xcom_pull(
            key="sample_xcom_key",
            task_ids='push_XCom'
        )
        print(f'XCom testing: {testing_increases}')

    push_xcom = PythonOperator(
        task_id = 'push_XCom',
        python_callable = push_xcom,
    )

    pull_xcom = PythonOperator(
        task_id = 'pull_XCom',
        python_callable = pull_xcom,
    )

    push_xcom >> pull_xcom
