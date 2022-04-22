from datetime import datetime, timedelta
from textwrap import dedent
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator


default_args = {
        'depends_on_past': False,
        'email': ['airflow@example.com'],
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5)
    }


with DAG(
    'krylov_task_8',
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    start_date=datetime(2022, 4, 20),
    catchup=False
) as dag:

    def set_xcom(ti):
        ti.xcom_push(
            key='sample_xcom_key',
            value='xcom test'
        )


    def get_xcom(ti):
        value = ti.xcom_pull(
            key='sample_xcom_key',
            task_ids='set_xcom'
        )
        print(value)


    t1 = PythonOperator(
        task_id='set_xcom',
        python_callable=set_xcom,
    )

    t2 = PythonOperator(
        task_id='get_xcom',
        python_callable=get_xcom,
    )

    t1 >> t2






