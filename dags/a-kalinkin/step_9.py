from datetime import datetime, timedelta
from textwrap import dedent

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python_operator import PythonOperator
from textwrap import dedent

with DAG(
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    'hw_9_a-kalinkin',#МЕНЯЙ ИМЯ ДАГА
    # !!!!!!!!!!!!!!!!!!!!!!!!!!!
    default_args={
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },

    description='DAG wiht XCom',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['hw_a-kalinkin'],
) as dag:

        def push_in_xcom(ti):
                ti.xcom_push(
                        key="sample_xcom_key",
                        value="xcom test"
                )


        def pull_in_xcom(ti):
                result=ti.xcom_pull(
                        key="sample_xcom_key",
                        task_ids='push_xcom'
                )
                return result

        push_xcom = PythonOperator(
                task_id='push_xcom',
                python_callable=push_in_xcom,
        )

        pull_xcom =  PythonOperator(
                task_id='pull_xcom',
                python_callable=pull_in_xcom,
        )

        push_xcom >> pull_xcom