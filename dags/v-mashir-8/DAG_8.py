from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

with DAG(
        'DAG_8',
        default_args={
            'depends_on_past': False,
            'email': ['airflow@example.com'],
            'email_on_failure': False,
            'email_on_retry': False,
            'retries': 1,
            'retry_delay': timedelta(minutes=5),
        },
        description='lesson_11_task_8',
        schedule_interval=timedelta(days=1),
        start_date=datetime(2022, 4, 11),
        catchup=False,
        tags=['v-mashir-8'],
) as dag:
    def push_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    t1 = PythonOperator(
        task_id='push_xcom',
        python_callable=push_xcom,
    )

    def pull_xcom(ti):
        xcom_test = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='push_xcom'
        )
        print(xcom_test)


    t2 = PythonOperator(
        task_id='pull_xcom',
        python_callable=pull_xcom,
    )
    t1 >> t2
